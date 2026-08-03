"""Recipe registry, versioning, provenance and skew (issue #145 M12.10).

The registry is the thing that makes "which code is executable here" provable, so
these tests care about three properties above all: entry kinds are distinguished
mechanically, discovery is independent of registration order, and provenance is
derived from code rather than accepted from anyone.
"""

import json
import os
import random
import re
import sys
import tempfile
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.build_info import (
    BUILD_REVISION_PATH,
    image_build_revision,
    is_source_digest,
    source_digest,
    source_revision,
)
from boomi_mcp.errors import (
    RECIPE_CAPABILITY_GATED,
    RECIPE_NOT_FOUND,
    RECIPE_VERSION_UNAVAILABLE,
)
from boomi_mcp.recipes import RecipeError, RecipeInputBase, production_registry
from boomi_mcp.recipes.registry import build_test_registry
from boomi_mcp.recipes import registry as registry_module
from boomi_mcp.recipes.builtins import catalog
from boomi_mcp.recipes.builtins.catalog import (
    PRODUCTION_REGISTRATIONS,
    RECIPE_ADVISORY_INTEGRATION_DESIGN,
    RECIPE_API_TO_API_SYNC,
    RECIPE_CONSTRAINT_INBOUND_VALIDATE,
    RECIPE_DB_REST_FANOUT,
)
from boomi_mcp.recipes.contracts import (
    ExpectedRecipeEntryV1,
    ExpectedRecipeRegistryV1,
    RecipeCapabilityRequirementV1,
    RecipeConflictPolicyV1,
    RecipeReferenceV1,
    RecipeRegistrationV1,
    parse_semver,
)
from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1, emit_api_to_api_sync


def _inherited_env():
    """The caller's environment, copied. Separate from :func:`_clean_env`, which
    strips ``PYTHONPATH`` — the tracer driver NEEDS it set."""
    return dict(os.environ)


def _clean_env():
    """The child's environment with ``PYTHONPATH`` REMOVED.

    A bare ``subprocess.run`` inherits it, and this repo is normally driven with
    ``PYTHONPATH=src`` — so the child could resolve ``boomi_mcp.*`` through the
    shell and the namespace isolation these probes rely on came from the caller,
    not from the test. An absolute import added to an owned module passed
    (issue #145, live QA).
    """
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    return env


def _reg(**kwargs):
    base = dict(
        recipe_id="test.recipe",
        recipe_version="1.0.0",
        entry_kind="executable_recipe",
        is_default=True,
        input_model=SyncRecipeInputV1,
        executor=emit_api_to_api_sync,
        output_types=(
            "component_contribution",
            "constraint_requirement",
            "process_ir_patch",
        ),
        conflict_policy=RecipeConflictPolicyV1(),
    )
    base.update(kwargs)
    return RecipeRegistrationV1(**base)


# ---------------------------------------------------------------------------
# SemVer
# ---------------------------------------------------------------------------


def test_semver_orders_numerically_not_lexically():
    """The classic trap: ``1.10.0`` must sort AFTER ``1.9.0``."""
    versions = ["1.9.0", "1.10.0", "1.2.0", "2.0.0", "1.0.0"]
    assert sorted(versions, key=parse_semver) == [
        "1.0.0",
        "1.2.0",
        "1.9.0",
        "1.10.0",
        "2.0.0",
    ]


def test_a_prerelease_sorts_before_its_release():
    assert parse_semver("1.0.0-alpha") < parse_semver("1.0.0")
    assert parse_semver("1.0.0-alpha.1") < parse_semver("1.0.0-alpha.beta")
    assert parse_semver("1.0.0-2") < parse_semver("1.0.0-rc")


def test_build_metadata_is_ignored_for_precedence():
    assert parse_semver("1.0.0+build.1") == parse_semver("1.0.0+build.2")


@pytest.mark.parametrize("bad", ["1", "1.0", "v1.0.0", "1.0.0.0", "01.0.0", ""])
def test_an_invalid_semver_is_rejected_at_construction(bad):
    with pytest.raises(ValueError):
        build_test_registry((_reg(recipe_version=bad),))


@pytest.mark.parametrize(
    "bad",
    [
        # Python's `\d` is Unicode by default; the published regex is written for
        # a dialect where it is ASCII. `[1-9]` takes the ASCII `1`, `\d*` takes
        # the Arabic-Indic digit, and `int()` happily parses the pair as 12.
        "1\N{ARABIC-INDIC DIGIT TWO}.0.0",
        "0.\N{ARABIC-INDIC DIGIT ONE}.0",
        # Python's `$` also matches just before a trailing newline, so this
        # registered as a version DISTINCT from "1.2.3" that compared exactly
        # EQUAL to it — two registry keys, one precedence.
        "1.2.3\n",
        "1.2.3-alpha\n",
    ],
)
def test_a_semver_lookalike_from_the_python_regex_dialect_is_rejected(bad):
    """The two ways a verbatim transcription of the official regex is wrong.

    Both were accepted, and the constant's own comment called itself "the
    official SemVer 2.0.0 regex" while behaving as neither (issue #145, §6
    architect review).
    """
    with pytest.raises(ValueError):
        parse_semver(bad)
    with pytest.raises(ValueError):
        build_test_registry((_reg(recipe_version=bad),))


def test_the_trailing_newline_lookalike_would_have_compared_equal():
    """Why the newline case is a defect and not a curiosity: it is not merely an
    odd string, it is an INDISTINGUISHABLE one — same precedence, different key.
    """
    assert _SEMVER_RE_UNPATCHED_WOULD_MATCH("1.2.3\n")
    with pytest.raises(ValueError):
        parse_semver("1.2.3\n")


def _SEMVER_RE_UNPATCHED_WOULD_MATCH(value: str) -> bool:
    """Rebuild the pre-fix regex to show the guard above is not vacuous."""
    import re

    from boomi_mcp.recipes.contracts import _SEMVER_RE

    unpatched = re.compile(_SEMVER_RE.pattern.replace(r"\Z", "$"))
    return unpatched.match(value) is not None


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_an_exact_version_resolves():
    registry = build_test_registry((_reg(),))
    assert registry.resolve("test.recipe", "1.0.0").recipe_version == "1.0.0"


def test_a_name_only_call_selects_the_code_declared_default():
    registry = build_test_registry(
        (
            _reg(recipe_version="1.0.0", is_default=False),
            _reg(recipe_version="2.0.0", is_default=True),
        )
    )
    assert registry.resolve("test.recipe").recipe_version == "2.0.0"


def test_an_exact_request_never_falls_forward_or_backward():
    """A silent fall-forward defeats the entire point of pinning."""
    registry = build_test_registry(
        (_reg(recipe_version="1.0.0"), _reg(recipe_version="2.0.0", is_default=False))
    )
    with pytest.raises(RecipeError) as exc:
        registry.resolve("test.recipe", "1.5.0")
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == RECIPE_VERSION_UNAVAILABLE
    assert diagnostic.available_versions == ("1.0.0", "2.0.0")


def test_an_unknown_recipe_id_is_not_found():
    registry = build_test_registry((_reg(),))
    with pytest.raises(RecipeError) as exc:
        registry.resolve("nope")
    assert exc.value.diagnostics[0].code == RECIPE_NOT_FOUND


def test_parallel_versions_coexist():
    registry = build_test_registry(
        (_reg(recipe_version="1.0.0"), _reg(recipe_version="2.0.0", is_default=False))
    )
    assert registry.versions_for("test.recipe") == ("1.0.0", "2.0.0")


def test_two_defaults_for_one_id_fail_at_construction():
    with pytest.raises(ValueError, match="default"):
        build_test_registry(
            (_reg(recipe_version="1.0.0"), _reg(recipe_version="2.0.0"))
        )


def test_a_duplicate_registration_fails_at_construction():
    with pytest.raises(ValueError, match="duplicate"):
        build_test_registry((_reg(), _reg(is_default=False)))


# ---------------------------------------------------------------------------
# Entry-kind invariants
# ---------------------------------------------------------------------------


def test_advisory_may_declare_no_executor_input_or_output():
    """The STRUCTURAL reason doctrine can never become executable."""
    for kwargs in (
        {"executor": emit_api_to_api_sync},
        {"input_model": SyncRecipeInputV1},
        {"output_types": ("constraint_requirement",)},
        {"adapter_target": RecipeReferenceV1(recipe_id="x.y", recipe_version="1.0.0")},
        {"conflict_policy": RecipeConflictPolicyV1()},
    ):
        base = dict(
            entry_kind="advisory",
            executor=None,
            input_model=None,
            output_types=(),
            conflict_policy=None,
        )
        base.update(kwargs)
        with pytest.raises(ValueError, match="advisory"):
            build_test_registry((_reg(**base),))


def test_an_advisory_entry_registers_cleanly_with_nothing_attached():
    registry = build_test_registry(
        (
            _reg(
                entry_kind="advisory",
                executor=None,
                input_model=None,
                output_types=(),
                conflict_policy=None,
            ),
        )
    )
    descriptor = registry.resolve("test.recipe")
    assert descriptor.entry_kind == "advisory"
    assert descriptor.output_types == ()
    with pytest.raises(RecipeError):
        registry.executor_for(descriptor)


def test_constraint_only_may_declare_only_constraint_output():
    with pytest.raises(ValueError, match="constraint_only"):
        build_test_registry(
            (
                _reg(
                    entry_kind="constraint_only",
                    output_types=("component_contribution", "constraint_requirement"),
                ),
            )
        )


def test_a_compatibility_adapter_needs_an_exact_target_and_no_executor():
    with pytest.raises(ValueError, match="adapter target"):
        build_test_registry(
            (
                _reg(
                    entry_kind="compatibility_adapter",
                    executor=None,
                    input_model=None,
                    output_types=(),
                    conflict_policy=None,
                    adapter_target=None,
                ),
            )
        )
    with pytest.raises(ValueError, match="compatibility_adapter"):
        build_test_registry(
            (
                _reg(
                    entry_kind="compatibility_adapter",
                    conflict_policy=None,
                    adapter_target=RecipeReferenceV1(
                        recipe_id="x.y", recipe_version="1.0.0"
                    ),
                ),
            )
        )


def test_an_executable_recipe_must_declare_at_least_one_output():
    with pytest.raises(ValueError, match="output type"):
        build_test_registry((_reg(output_types=()),))


def test_an_unknown_output_type_fails_at_construction():
    with pytest.raises(ValueError, match="unknown output type"):
        build_test_registry((_reg(output_types=("nonsense",)),))


def test_output_types_must_be_sorted_and_unique():
    with pytest.raises(ValueError, match="sorted"):
        build_test_registry(
            (_reg(output_types=("process_ir_patch", "component_contribution")),)
        )
    with pytest.raises(ValueError, match="duplicate"):
        build_test_registry(
            (_reg(output_types=("process_ir_patch", "process_ir_patch")),)
        )


def test_an_input_model_must_subclass_recipe_input_base():
    from pydantic import BaseModel

    class Loose(BaseModel):
        pass

    with pytest.raises(ValueError, match="RecipeInputBase"):
        build_test_registry((_reg(input_model=Loose),))


def test_a_subclass_that_reopens_extra_is_rejected_despite_correct_ancestry():
    """Ancestry is not shape. ``extra`` is inherited config — and overridable.

    ``issubclass(..., RecipeInputBase)`` was the whole check, so a model with
    impeccable ancestry could re-open itself in one line and register unchanged,
    carrying exactly the unbounded object the recipe input contract forbids
    (issue #145, §6 architect review).
    """
    from pydantic import ConfigDict

    class Reopened(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)

        process_key: str = "p"

    assert issubclass(Reopened, RecipeInputBase)  # the old check still passes
    with pytest.raises(ValueError, match="not closed"):
        build_test_registry((_reg(input_model=Reopened),))


def test_an_unbounded_dict_field_is_rejected():
    """``Dict[str, Any]`` is an open object by another route, and closed config
    does not close it — the field's own schema is what carries
    ``additionalProperties``."""
    from typing import Any, Dict

    class WithBag(RecipeInputBase):
        process_key: str = "p"
        bag: Dict[str, Any] = {}

    with pytest.raises(ValueError, match="not closed"):
        build_test_registry((_reg(input_model=WithBag),))


def test_a_nested_open_model_is_rejected_even_when_the_top_level_is_closed():
    """The walk covers ``$defs``. A closed envelope around an open payload is
    open, and only the nested node says so."""
    from pydantic import BaseModel, ConfigDict

    class OpenLeaf(BaseModel):
        model_config = ConfigDict(extra="allow")

        name: str = "x"

    class ClosedEnvelope(RecipeInputBase):
        process_key: str = "p"
        leaf: OpenLeaf = OpenLeaf()

    with pytest.raises(ValueError, match="not closed"):
        build_test_registry((_reg(input_model=ClosedEnvelope),))


def test_a_nested_model_with_pydantic_default_config_is_rejected():
    """The rule is ``is not False``, not ``is True``, and this is why.

    Pydantic's DEFAULT config (``extra="ignore"``) emits NO
    ``additionalProperties`` key, so a nested model written without
    ``ConfigDict(extra="forbid")`` — the likeliest way an open shape actually
    reaches a registration — is caught only by treating absent as open. Every
    other closedness test uses ``extra="allow"`` or ``Dict[str, Any]``, both of
    which emit ``additionalProperties: true`` and would be caught by either
    rule, so nothing pinned the branch that matters most (issue #145, live QA).
    """
    from pydantic import BaseModel

    class DefaultConfigLeaf(BaseModel):  # no ConfigDict at all
        name: str = "x"

    assert "additionalProperties" not in DefaultConfigLeaf.model_json_schema()

    class Envelope(RecipeInputBase):
        process_key: str = "p"
        leaf: DefaultConfigLeaf = DefaultConfigLeaf()

    with pytest.raises(ValueError, match="not closed"):
        build_test_registry((_reg(input_model=Envelope),))


@pytest.mark.parametrize(
    "annotation",
    ["bare_any", "any_list"],
)
def test_an_unconstrained_field_is_rejected(annotation):
    """An unconstrained FIELD never passes through an object node.

    ``thing: Any`` compiles to a property schema with no ``type``, no ``$ref``
    and no combinator — it accepts an arbitrary object while presenting nothing
    for the ``additionalProperties`` rule to test. ``List[Any]`` hides the same
    schema one level in, under ``items``.
    """
    from typing import Any as AnyType
    from typing import List as ListType

    if annotation == "bare_any":

        class Model(RecipeInputBase):
            process_key: str = "p"
            thing: AnyType = None

    else:

        class Model(RecipeInputBase):
            process_key: str = "p"
            things: ListType[AnyType] = []

    with pytest.raises(ValueError, match="unconstrained field"):
        build_test_registry((_reg(input_model=Model),))


@pytest.mark.parametrize(
    "shape",
    ["tuple_any", "optional_any", "union_any", "ref_to_empty_def"],
)
def test_an_unconstrained_schema_is_caught_in_every_position_it_can_hide(shape):
    """``properties/*`` and ``items`` are not the only places a schema sits.

    An empty schema can also be a ``prefixItems`` member, an ``anyOf`` member, or
    the ``$defs`` entry a ``$ref`` resolves to. In each of those the PROPERTY
    itself looks bounded — it carries ``prefixItems`` / ``anyOf`` / ``$ref`` — so
    a rule that only inspected the property node accepted all four, and
    ``Optional[Any]`` and pydantic's ``JsonValue`` are ordinary things a
    developer writes (issue #145, live QA).
    """
    from typing import Any as AnyType
    from typing import Optional as OptionalType
    from typing import Tuple as TupleType
    from typing import Union as UnionType

    if shape == "tuple_any":

        class Model(RecipeInputBase):
            t: TupleType[AnyType, str] = (None, "s")

    elif shape == "optional_any":

        class Model(RecipeInputBase):
            o: OptionalType[AnyType] = None

    elif shape == "union_any":

        class Model(RecipeInputBase):
            u: UnionType[AnyType, str] = "s"

    else:
        from pydantic import JsonValue

        class Model(RecipeInputBase):
            j: JsonValue = None

    with pytest.raises(ValueError, match="unconstrained"):
        build_test_registry((_reg(input_model=Model),))


@pytest.mark.parametrize(
    "shape", ["field_named_properties", "default_carrying_type_object", "extra_examples"]
)
def test_the_closedness_walk_never_treats_caller_DATA_as_schema(shape):
    """A JSON Schema document contains data as well as schema.

    ``default``, ``examples`` and ``json_schema_extra`` hold caller VALUES.
    Recursing over every dict value walked those as if they were schema, which
    was wrong in both directions at once: a closed model with an ordinary field
    named ``properties`` crashed the check with ``AttributeError: 'str' object
    has no attribute 'items'``, and a closed model whose default contained
    ``{"type": "object"}`` was rejected at ``/default``. A validator whose entire
    contract is "raise a clear ``ValueError``" must not raise ``AttributeError``
    instead (issue #145, live QA).
    """
    from pydantic import BaseModel, ConfigDict, Field

    if shape == "field_named_properties":

        class Leaf(BaseModel):
            model_config = ConfigDict(extra="forbid")

            properties: str = "p"  # collides with a schema keyword by name only

        class Model(RecipeInputBase):
            leaf: Leaf = Leaf()

    elif shape == "default_carrying_type_object":

        class Leaf(BaseModel):
            model_config = ConfigDict(extra="forbid")

            type: str = "object"  # a VALUE that looks like a schema fragment

        class Model(RecipeInputBase):
            leaf: Leaf = Leaf()

    else:

        class Model(RecipeInputBase):
            a: str = Field(
                "x", json_schema_extra={"examples": [{"properties": "p", "type": "object"}]}
            )

    # Registers cleanly: no crash, and no spurious rejection.
    registry = build_test_registry((_reg(input_model=Model),))
    assert registry.resolve("test.recipe").input_schema_id.endswith("Model")


@pytest.mark.parametrize(
    "keyword,fragment",
    [
        ("type", {"type": "string"}),
        ("$ref", {"$ref": "#/$defs/X"}),
        ("anyOf", {"anyOf": [{"type": "string"}]}),
        ("oneOf", {"oneOf": [{"type": "string"}]}),
        ("allOf", {"allOf": [{"type": "string"}]}),
        ("enum", {"enum": ["a", "b"]}),
        ("const", {"const": "a"}),
        ("properties", {"properties": {"a": {"type": "string"}}}),
        ("items", {"items": {"type": "string"}}),
        ("prefixItems", {"prefixItems": [{"type": "string"}]}),
        ("additionalProperties", {"additionalProperties": {"type": "string"}}),
    ],
)
def test_every_constraining_keyword_bounds_a_subschema_on_its_own(keyword, fragment):
    """Each member pinned individually, against a fragment carrying ONLY it.

    This is the only level at which the set is observable. In the schemas
    pydantic emits, nine of the twelve always appear alongside ``type`` — a
    ``Literal`` is ``{"enum": [...], "type": "string"}``, a tuple is
    ``{"prefixItems": [...], "type": "array"}`` — so ``type`` alone keeps those
    models bounded and dropping any of the nine changed nothing any registered
    model could detect. Removing a keyword makes the check STRICTER, so the
    defect it would cause is a spurious rejection, and only a fragment WITHOUT
    ``type`` can catch it (issue #145, live QA).
    """
    from boomi_mcp.recipes.registry import _is_bounded_subschema

    assert keyword in fragment
    assert "type" not in fragment or keyword == "type"
    assert _is_bounded_subschema(fragment) is True


@pytest.mark.parametrize(
    "fragment",
    [
        {},
        {"title": "Thing"},
        {"default": None, "title": "Thing", "description": "d"},
        {"examples": [1, 2]},
        {"deprecated": True},
    ],
)
def test_an_annotation_only_subschema_is_not_bounded(fragment):
    """Annotations describe; they do not constrain. The negative half of the
    set — without it every fragment could be "bounded" and the rule vacuous."""
    from boomi_mcp.recipes.registry import _is_bounded_subschema

    assert _is_bounded_subschema(fragment) is False


@pytest.mark.parametrize("value", [None, True, False, "nope", 17, []])
def test_a_non_mapping_subschema_is_not_bounded(value):
    """``additionalProperties: true`` is a bool, not a dict — the predicate must
    answer for non-mappings rather than raise."""
    from boomi_mcp.recipes.registry import _is_bounded_subschema

    assert _is_bounded_subschema(value) is False


def test_not_is_absent_from_the_constraining_set_because_it_bounds_nothing():
    """``not`` looks like a constraint and is not one.

    ``{"not": {"type": "string"}}`` means "anything except a string", which
    admits ``{"password": "..."}`` — exactly the unbounded object this check
    exists to reject. Listing it made an open schema register as bounded, and an
    earlier version of the predicate test asserted that as a PROPERTY, pinning
    the unsoundness rather than the rule (issue #145, live QA).
    """
    from boomi_mcp.recipes.registry import (
        _CONSTRAINING_KEYWORDS,
        _is_bounded_subschema,
    )

    assert "not" not in _CONSTRAINING_KEYWORDS
    assert _is_bounded_subschema({"not": {"type": "string"}}) is False


@pytest.mark.parametrize(
    "position,schema,pointer",
    [
        (
            "patternProperties",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "bag": {
                        "type": "object",
                        "additionalProperties": False,
                        "patternProperties": {"^x": {}},
                    }
                },
            },
            "/properties/bag/patternProperties/^x",
        ),
        (
            "allOf",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {"a": {"allOf": [{}]}},
            },
            "/properties/a/allOf/0",
        ),
        (
            "oneOf",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {"a": {"oneOf": [{}, {"type": "string"}]}},
            },
            "/properties/a/oneOf/0",
        ),
        (
            "propertyNames",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "bag": {
                        "type": "object",
                        "additionalProperties": False,
                        "propertyNames": {"items": {}},
                    }
                },
            },
            "/properties/bag/propertyNames/items",
        ),
    ],
)
def test_the_walker_descends_into_positions_no_model_can_exercise(
    position, schema, pointer
):
    """Some recursion positions cannot be reached through a pydantic model.

    ``patternProperties`` is the clearest case: whenever pydantic emits it, the
    parent object also lacks ``additionalProperties``, so the OBJECT rule flags
    the parent and raises before the unconstrained report is ever reached — the
    nested defect is masked by a coarser one. Driving the walker with a
    hand-written schema is the only way to observe the descent, exactly as the
    predicate test is the only way to observe the keyword set (issue #145, live
    QA).
    """

    class _StubModel:
        @staticmethod
        def model_json_schema():
            return schema

    with pytest.raises(ValueError) as exc:
        registry_module._check_input_schema_closed("probe", _StubModel)
    assert "unconstrained" in str(exc.value)
    assert pointer in str(exc.value), str(exc.value)


@pytest.mark.parametrize(
    "shape,pointer",
    [
        ("discriminated_union", "/$defs/VariantAny/properties/payload"),
        ("dict_of_any_list", "/properties/bag/additionalProperties/items"),
    ],
)
def test_an_unconstrained_schema_is_caught_in_the_reachable_nested_positions(
    shape, pointer
):
    """TWO positions, pinned through a real model, each WITH its pointer.

    ``dict_of_any_list`` (``Dict[str, List[Any]]``) pins the
    ``additionalProperties`` descent. ``discriminated_union`` pins the ``$defs``
    recursion — NOT ``oneOf``, despite the model being a discriminated union:
    pydantic emits ``oneOf`` members as ``$ref`` strings, so the variant's own
    schema is reached through ``$defs`` and the pointer says so.
    ``patternProperties`` is absent for a different reason — whenever pydantic
    emits it the parent object also lacks ``additionalProperties``, so the
    coarser object rule flags the parent and raises first. Both live in
    ``test_the_walker_descends_into_positions_no_model_can_exercise`` instead.

    An earlier version of this docstring claimed three positions including those
    two, which mutation attribution disproved: their drops die to the stub test,
    never to this one (issue #145, live QA).

    Asserting the POINTER as well as the failure is what makes these tests
    position-specific: without it every pointer in the message could be replaced
    by a constant and nothing would notice.
    """
    from typing import Any as AnyType
    from typing import Dict as DictType
    from typing import List as ListType
    from typing import Literal as LiteralType
    from typing import Union as UnionType

    from pydantic import BaseModel, ConfigDict, Field

    if shape == "discriminated_union":

        class VariantAny(BaseModel):
            model_config = ConfigDict(extra="forbid")

            kind: LiteralType["any"] = "any"
            payload: AnyType = None

        class VariantStr(BaseModel):
            model_config = ConfigDict(extra="forbid")

            kind: LiteralType["str"] = "str"
            payload: str = ""

        class Model(RecipeInputBase):
            u: UnionType[VariantAny, VariantStr] = Field(
                default_factory=VariantStr, discriminator="kind"
            )

    else:

        class Model(RecipeInputBase):
            bag: DictType[str, ListType[AnyType]] = {}

    with pytest.raises(ValueError) as exc:
        build_test_registry((_reg(input_model=Model),))
    assert "not closed" in str(exc.value)
    assert pointer in str(exc.value), str(exc.value)


@pytest.mark.parametrize(
    "case,schema",
    [
        (
            "unconstrained inside a negation is not a defect",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "a": {"type": "string", "not": {"properties": {"z": {}}}}
                },
            },
        ),
        (
            "propertyNames is descended into, never flagged",
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "bag": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                        "propertyNames": {"minLength": 1},
                    }
                },
            },
        ),
    ],
)
def test_the_walker_does_not_flag_what_it_only_descends_into(case, schema):
    """The other half of two decisions, and both are load-bearing.

    Removing ``not`` from the keyword set was only half of #368; the walk also
    stopped DESCENDING into it, because an unconstrained subschema inside a
    negation makes the parent stricter, not looser. Re-adding that descent was
    invisible to the suite.

    ``propertyNames`` is the mirror image: it bounds KEY names, and its schema
    is routinely ``{"minLength": 1}`` — which carries no constraining keyword,
    so flagging instead of descending would REJECT
    ``Dict[Annotated[str, StringConstraints(min_length=1)], str]``, a perfectly
    legitimate model. Swapping the descend for a check was invisible too
    (issue #145, live QA).
    """

    class _StubModel:
        @staticmethod
        def model_json_schema():
            return schema

    registry_module._check_input_schema_closed("probe", _StubModel)  # must not raise


def test_a_constrained_key_mapping_registers():
    """The model-level half of the ``propertyNames`` case above.

    ``Dict[Annotated[str, StringConstraints(min_length=1)], str]`` is what makes
    pydantic emit ``propertyNames``, and it must register.
    """
    from typing import Dict as DictType

    from pydantic import StringConstraints
    from typing_extensions import Annotated

    class Model(RecipeInputBase):
        bag: DictType[Annotated[str, StringConstraints(min_length=1)], str] = {}

    assert "propertyNames" in json.dumps(Model.model_json_schema())
    registry = build_test_registry((_reg(input_model=Model),))
    assert registry.resolve("test.recipe").input_schema_id.endswith("Model")


def test_an_object_is_recognised_by_its_properties_when_it_declares_no_type():
    """The object rule is ``type == "object"`` OR ``"properties" in node``.

    Only the first half was pinned. A node carrying ``properties`` without a
    ``type`` is still an object, and dropping the second half let it through
    unjudged (issue #145, live QA).
    """
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"a": {"properties": {"b": {"type": "string"}}}},
    }

    class _StubModel:
        @staticmethod
        def model_json_schema():
            return schema

    with pytest.raises(ValueError) as exc:
        registry_module._check_input_schema_closed("probe", _StubModel)
    assert "additionalProperties must be false" in str(exc.value)
    assert "/properties/a" in str(exc.value), str(exc.value)


def test_an_open_object_is_reported_at_the_node_that_is_open():
    """The offender pointer, pinned.

    Every closedness test matched on the message text alone, so both pointer
    lists could be replaced by a constant ``"/"`` with the suite green — the
    diagnostic would still fire, and still name the wrong place. A registration
    error that says "somewhere in this schema" is barely better than none
    (issue #145, live QA).
    """
    from pydantic import BaseModel, ConfigDict

    class OpenLeaf(BaseModel):
        model_config = ConfigDict(extra="allow")

        name: str = "x"

    class Envelope(RecipeInputBase):
        process_key: str = "p"
        leaf: OpenLeaf = OpenLeaf()

    with pytest.raises(ValueError) as exc:
        build_test_registry((_reg(input_model=Envelope),))
    message = str(exc.value)
    assert "additionalProperties must be false" in message
    # The NESTED model is what is open — not the closed top level.
    assert "/$defs/OpenLeaf" in message, message
    assert "['/']" not in message, message


def test_a_model_that_allows_typed_extras_is_still_rejected():
    """Bounding the VALUE type of an undeclared key is not declaring the key.

    ``extra="allow"`` plus a typed ``__pydantic_extra__: Dict[str, str]`` makes
    pydantic emit ``additionalProperties: {"type": "string"}`` on the MODEL node
    — the same shape a ``Dict[str, str]`` field emits — so the mapping rule read
    it as bounded while ``model_validate`` happily preserved
    ``{"smuggled": "value"}``. Undeclared caller data reaching an executor is
    exactly what the closed input contract exists to prevent
    (issue #145, Codex review).

    ``properties`` is the discriminator: pydantic emits it for every model,
    including a field-less one, and never for a mapping field.
    """
    from typing import Dict as DictType

    from pydantic import ConfigDict

    class ExtraAllowed(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)
        __pydantic_extra__: DictType[str, str]

        process_key: str = "p"

    # The hole was real: validation genuinely keeps the undeclared key.
    kept = ExtraAllowed.model_validate({"process_key": "p", "smuggled": "value"})
    assert kept.model_dump()["smuggled"] == "value"

    schema = ExtraAllowed.model_json_schema()
    assert schema["additionalProperties"] == {"type": "string"}
    assert "properties" in schema  # ...which is what marks it a model node

    with pytest.raises(ValueError, match="additionalProperties must be false"):
        build_test_registry((_reg(input_model=ExtraAllowed),))


@pytest.mark.parametrize(
    "keyword,empty_value",
    [
        ("allOf", []),
        ("anyOf", []),
        ("oneOf", []),
        ("prefixItems", []),
        ("enum", []),
        ("properties", {}),
        ("items", {}),
        ("additionalProperties", {}),
        ("type", ""),
        ("$ref", ""),
    ],
)
def test_a_constraining_keyword_at_an_empty_value_bounds_nothing(keyword, empty_value):
    """Presence is not constraint.

    ``{"allOf": []}`` carries a keyword from the set and imposes nothing — an
    empty conjunction is vacuously true — so
    ``Annotated[Any, WithJsonSchema({"allOf": []})]`` passed a name-only check
    while accepting arbitrary dicts, lists and scalars. The predicate now judges
    each keyword AT ITS VALUE (issue #145, Codex review).
    """
    from boomi_mcp.recipes.registry import _is_bounded_subschema

    assert _is_bounded_subschema({keyword: empty_value}) is False


def test_an_empty_applicator_on_a_real_field_is_rejected():
    """The model-level half of the case above, through the real entry point."""
    from typing import Any as AnyType

    from pydantic import WithJsonSchema
    from typing_extensions import Annotated

    class Model(RecipeInputBase):
        process_key: str = "p"
        thing: Annotated[AnyType, WithJsonSchema({"allOf": []})] = None

    # The hole was real: the field genuinely accepts an arbitrary object.
    assert Model.model_validate({"thing": {"password": "hunter2"}}).thing == {
        "password": "hunter2"
    }
    with pytest.raises(ValueError, match="unconstrained"):
        build_test_registry((_reg(input_model=Model),))


def test_a_non_empty_applicator_still_bounds():
    """The control. Without it, judging keywords by value could pass every case
    above by simply calling everything unbounded."""
    from boomi_mcp.recipes.registry import _is_bounded_subschema

    assert _is_bounded_subschema({"allOf": [{"type": "string"}]}) is True
    assert _is_bounded_subschema({"enum": ["a"]}) is True
    assert _is_bounded_subschema({"items": {"type": "string"}}) is True
    assert _is_bounded_subschema({"additionalProperties": {"type": "string"}}) is True
    # ``const`` bounds at ANY value — including None and False, which are single
    # permitted values, not absent ones.
    assert _is_bounded_subschema({"const": None}) is True
    assert _is_bounded_subschema({"const": False}) is True


def test_the_bounded_predicate_terminates_on_a_self_referential_schema():
    """``items``/``additionalProperties`` recurse, so the depth cap is load-bearing
    for hand-written schemas even though pydantic emits trees."""
    from boomi_mcp.recipes.registry import _is_bounded_subschema

    cyclic: dict = {}
    cyclic["items"] = cyclic
    assert _is_bounded_subschema(cyclic) is False


def test_a_mapping_with_no_additional_properties_is_rejected_at_its_own_pointer():
    """The MAPPING branch of the object rule, which the model/mapping split
    silently stopped covering.

    Splitting one rule into two branches migrated every existing test to the
    model branch — they all emit ``properties`` — so the mapping branch was
    decided by no test at all, and ``is not False`` could be weakened back to
    ``is True`` with the suite green. That mutant was killed two rounds ago; the
    split regressed it (issue #145, live QA).

    ``Dict[constr(pattern=...), V]`` is the shape that reaches it: pydantic emits
    ``patternProperties`` with NO ``additionalProperties``, so keys not matching
    the pattern are unbounded. Asserting the pointer pins the mapping branch's
    offender report too, which was likewise unpinned.
    """
    from typing import Dict as DictType

    from pydantic import StringConstraints
    from typing_extensions import Annotated

    class Model(RecipeInputBase):
        a: DictType[Annotated[str, StringConstraints(pattern=r"^x")], str] = {}

    node = Model.model_json_schema()["properties"]["a"]
    assert "patternProperties" in node
    assert "additionalProperties" not in node  # the mapping branch's live case
    assert "properties" not in node  # ...and it is NOT a model node

    with pytest.raises(ValueError) as exc:
        build_test_registry((_reg(input_model=Model),))
    assert "additionalProperties must be false" in str(exc.value)
    assert "/properties/a" in str(exc.value), str(exc.value)


def test_an_input_model_must_forbid_extras_as_a_validation_fact():
    """A published schema cannot be trusted to describe what validation accepts.

    ``extra="allow"`` plus ``json_schema_extra={"additionalProperties": False}``
    publishes a closed schema and keeps every undeclared key anyway — the schema
    walk has nothing to catch, because the schema is not lying about itself, it
    is lying about the validator. Reading ``model_config`` instead is the only
    way to see it (issue #145, live QA).
    """
    from pydantic import ConfigDict

    class Fabricated(RecipeInputBase):
        model_config = ConfigDict(
            extra="allow",
            frozen=True,
            json_schema_extra={"additionalProperties": False},
        )

        process_key: str = "p"

    # The schema looks closed...
    assert Fabricated.model_json_schema()["additionalProperties"] is False
    # ...and validation keeps the undeclared key regardless.
    assert (
        Fabricated.model_validate({"process_key": "p", "smuggled": "v"}).model_dump()[
            "smuggled"
        ]
        == "v"
    )

    with pytest.raises(ValueError, match="extra='forbid'"):
        build_test_registry((_reg(input_model=Fabricated),))


def test_a_fabricated_json_schema_hook_cannot_launder_open_extras_either():
    """The same lie told through ``__get_pydantic_json_schema__``."""
    from typing import Any as AnyType

    from pydantic import ConfigDict

    class HookFabricated(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)

        process_key: str = "p"

        @classmethod
        def __get_pydantic_json_schema__(cls, core_schema, handler) -> AnyType:
            schema = handler(core_schema)
            schema["additionalProperties"] = False
            return schema

    assert HookFabricated.model_json_schema()["additionalProperties"] is False
    with pytest.raises(ValueError, match="extra='forbid'"):
        build_test_registry((_reg(input_model=HookFabricated),))


def test_every_production_input_model_forbids_extras():
    """The guard must ACCEPT what ships, or it is a build break not a check.

    Drives the GATE rather than re-implementing its predicate. The earlier
    version asserted ``model_config.get("extra") == "forbid"`` itself, which
    made it blind twice over: it would not have noticed the gate being deleted,
    and it restated exactly the declaration the gate no longer trusts
    (issue #145, live QA).
    """
    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS

    checked = 0
    for registration in PRODUCTION_REGISTRATIONS:
        if registration.input_model is None:
            continue
        registry_module._check_input_model_forbids_extras(
            registration.recipe_id, registration.input_model
        )  # must not raise
        checked += 1
    # A census pin: a fifth input model must come with a deliberate update here.
    assert checked == 4


@pytest.mark.parametrize("lie", ["post_hoc_edit", "replaced_config", "lying_get"])
def test_a_config_that_merely_declares_forbid_does_not_satisfy_the_gate(lie):
    """``model_config["extra"]`` is a DECLARATION, not what the validator does.

    It is a mutable class attribute read at check time, while the validator was
    compiled at class construction — so an edit after the fact moves the
    declaration and not the validator. All three of these read ``forbid`` and
    all three keep undeclared keys. Running one probe key through
    ``model_validate`` cannot be fooled by any of them (issue #145, live QA).
    """
    from pydantic import ConfigDict

    class Base(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)

        process_key: str = "p"

    if lie == "post_hoc_edit":
        model = Base
        model.model_config["extra"] = "forbid"  # no model_rebuild()
    elif lie == "replaced_config":
        model = Base
        model.model_config = dict(model.model_config, extra="forbid")
    else:

        class LyingConfig(dict):
            def get(self, key, default=None):
                return "forbid" if key == "extra" else super().get(key, default)

        model = Base
        model.model_config = LyingConfig(model.model_config)

    # The declaration says closed...
    assert model.model_config.get("extra") == "forbid"
    # ...and the validator disagrees.
    assert "smuggled" in model.model_validate(
        {"process_key": "p", "smuggled": "v"}
    ).model_dump()

    with pytest.raises(ValueError, match="must reject undeclared keys"):
        registry_module._check_input_model_forbids_extras("probe", model)


def test_an_open_model_with_a_required_field_is_still_rejected():
    """A ``ValidationError`` is not itself evidence of closedness.

    The probe payload omits every declared field, so a model with a REQUIRED
    field raises ``ValidationError`` whatever its ``extra`` setting. Accepting
    on "an error was raised" would therefore pass an ``extra="allow"`` model
    purely because it had a required field — the error would be
    ``missing``, not ``extra_forbidden``. Only the specific error code answers
    the question being asked (issue #145, live QA).
    """
    from pydantic import ConfigDict, ValidationError

    class OpenWithRequired(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)

        needed: str

    # It DOES raise — just not for the reason closedness needs.
    with pytest.raises(ValidationError) as raised:
        OpenWithRequired.model_validate({"__closedness_probe__": "x"})
    codes = {error["type"] for error in raised.value.errors()}
    assert "missing" in codes
    assert "extra_forbidden" not in codes
    # ...and it genuinely keeps undeclared keys.
    assert "smuggled" in OpenWithRequired.model_validate(
        {"needed": "n", "smuggled": "v"}
    ).model_dump()

    with pytest.raises(ValueError, match="must reject undeclared keys"):
        registry_module._check_input_model_forbids_extras("probe", OpenWithRequired)


@pytest.mark.parametrize("where", ["empty_loc", "nested_loc"])
def test_a_fabricated_extra_forbidden_elsewhere_does_not_satisfy_the_probe(where):
    """The probe checks the error's LOCATION, not only its code.

    A ``model_validator`` can raise ``PydanticCustomError("extra_forbidden", …)``
    with an empty or nested ``loc``. Alone that is not a bypass — the schema gate
    still sees ``extra="allow"``. Combined with a fabricated
    ``json_schema_extra={"additionalProperties": False}`` it passes BOTH gates
    while the model keeps every undeclared key: the two gates exist to cover each
    other, and this is the one arrangement where neither does
    (issue #145, live QA).
    """
    from typing import Any as AnyType

    from pydantic import ConfigDict, model_validator
    from pydantic_core import PydanticCustomError

    if where == "empty_loc":

        class Composite(RecipeInputBase):
            model_config = ConfigDict(
                extra="allow",
                frozen=True,
                json_schema_extra={"additionalProperties": False},
            )

            process_key: str = "p"

            @model_validator(mode="before")
            @classmethod
            def _fake(cls, data: AnyType) -> AnyType:
                if isinstance(data, dict) and "__closedness_probe__" in data:
                    raise PydanticCustomError("extra_forbidden", "nope")
                return data

    else:

        class Inner(RecipeInputBase):
            ok: str = "x"

        class Composite(RecipeInputBase):
            model_config = ConfigDict(
                extra="allow",
                frozen=True,
                json_schema_extra={"additionalProperties": False},
            )

            process_key: str = "p"

            @model_validator(mode="before")
            @classmethod
            def _fake(cls, data: AnyType) -> AnyType:
                if isinstance(data, dict) and "__closedness_probe__" in data:
                    Inner.model_validate({"bogus": 1})  # extra_forbidden, nested loc
                return data

    # The SCHEMA gate is fooled by the json_schema_extra lie...
    registry_module._check_input_schema_closed("probe", Composite)  # does not raise
    # ...and the model really does keep undeclared keys.
    assert "smuggled" in Composite.model_validate(
        {"process_key": "p", "smuggled": "v"}
    ).model_dump()
    # ...so the extras gate is the only thing standing, and it must hold. It
    # does so without consulting the forged error at all: the compiled core
    # schema still says ``extra="allow"``, and no validator can rewrite that.
    with pytest.raises(ValueError, match="must reject undeclared keys"):
        registry_module._check_input_model_forbids_extras("probe", Composite)


def test_the_gate_accepts_an_honest_model_via_its_compiled_core_schema():
    """The positive half: an honest model is accepted, and via the core schema.

    Without this, "read the core schema" would be satisfiable by rejecting
    everything — which is exactly what the naive one-liner did to
    ``ComposeDbRestFanoutInputV1`` before the wrapper walk was added.
    """
    class Honest(RecipeInputBase):
        process_key: str = "p"

    core = Honest.__pydantic_core_schema__
    assert core["config"]["extra_fields_behavior"] == "forbid"
    registry_module._check_input_model_forbids_extras("probe", Honest)  # accepts


@pytest.mark.parametrize("behaviour", ["allow", "ignore"])
def test_only_forbid_satisfies_the_gate(behaviour):
    """``ignore`` is not ``forbid``, and only ``forbid`` is closed.

    ``extra="ignore"`` DROPS undeclared keys rather than passing them through, so
    it does not leak — which is precisely why nothing caught that widening the
    accept condition to ``("forbid", "ignore")`` left the suite green. It is
    still not the contract: the recipe input surface rejects what it does not
    declare, so a caller who misspells a field is told, rather than silently
    getting a default (issue #145, live QA).
    """
    from pydantic import ConfigDict

    class Model(RecipeInputBase):
        model_config = ConfigDict(extra=behaviour, frozen=True)

        process_key: str = "p"

    assert (
        Model.__pydantic_core_schema__["config"]["extra_fields_behavior"] == behaviour
    )
    with pytest.raises(ValueError, match="must reject undeclared keys"):
        registry_module._check_input_model_forbids_extras("probe", Model)


def test_the_gate_walks_past_wrapper_nodes_to_find_the_model_config():
    """REQUIRED, not defensive — and a production model depends on it.

    A ``model_validator(mode="after")`` wraps the model node in
    ``{"type": "function-after", "config": None, "schema": {...}}``. Reading only
    the top level finds ``config: None``, concludes nothing, and fail-closed then
    REJECTS a legitimate model. ``ComposeDbRestFanoutInputV1`` has exactly this
    shape, so the naive version is a build break (issue #145, live QA).
    """
    from pydantic import model_validator

    class Wrapped(RecipeInputBase):
        a: str = "x"

        @model_validator(mode="after")
        def _after(self):
            return self

    core = Wrapped.__pydantic_core_schema__
    assert core["type"] == "function-after"
    assert core.get("config") is None  # the top level says nothing
    assert core["schema"]["config"]["extra_fields_behavior"] == "forbid"
    registry_module._check_input_model_forbids_extras("probe", Wrapped)  # accepts

    from boomi_mcp.recipes.builtins.sync import SyncRecipeInputV1  # noqa: F401
    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS

    wrapped_production = [
        registration
        for registration in PRODUCTION_REGISTRATIONS
        if registration.input_model is not None
        and registration.input_model.__pydantic_core_schema__.get("config") is None
    ]
    assert wrapped_production, "no production model exercises the wrapper walk"


def test_a_wrap_validator_that_can_skip_its_handler_fails_closed():
    """A wrapper is not automatically passive, and this one is not.

    ``mode="wrap"`` receives the handler and may simply not call it. Pydantic
    still compiles the inner model node with ``extra_fields_behavior="forbid"``,
    so reading the inner config accepts the model — while validation never runs,
    ``model_validate`` returns the RAW MAPPING instead of an instance, and every
    undeclared key reaches the executor. Whether the handler is invoked is a fact
    about arbitrary Python, not about the schema, so the only sound answer is
    "cannot determine" (issue #145, Codex review).
    """
    from typing import Any as AnyType

    from pydantic import model_validator

    class WrapBypass(RecipeInputBase):
        a: str = "x"

        @model_validator(mode="wrap")
        @classmethod
        def _wrap(cls, data: AnyType, handler) -> AnyType:
            return data  # handler deliberately not invoked

    # The inner config still says forbid — which is exactly why reading it is
    # not enough.
    inner = WrapBypass.__pydantic_core_schema__["schema"]
    assert inner["config"]["extra_fields_behavior"] == "forbid"
    # ...and the hole is real: not even a model instance comes back.
    escaped = WrapBypass.model_validate({"a": "x", "smuggled": "value"})
    assert isinstance(escaped, dict)
    assert escaped["smuggled"] == "value"

    with pytest.raises(ValueError, match="mode='wrap' or mode='plain'"):
        build_test_registry((_reg(input_model=WrapBypass),))


@pytest.mark.parametrize(
    "placement", ["nested", "two_deep", "in_list", "in_dict", "optional", "in_union"]
)
def test_a_wrap_validator_is_caught_wherever_it_sits(placement):
    """The gate walks the WHOLE tree, not the root spine.

    Walking only ``schema`` from the root stops at the first model config and
    never looks at fields — so a NESTED model carrying a handler-skipping
    ``mode="wrap"`` validator passed both gates while its field came back as a
    raw mapping with every undeclared key intact. The JSON-schema gate cannot
    catch it either: the nested model still publishes
    ``additionalProperties: false``, because it really is declared closed. What
    is bypassed is the validation, not the declaration (issue #145, Codex
    review).
    """
    from typing import Any as AnyType
    from typing import Dict as DictType
    from typing import List as ListType
    from typing import Optional as OptionalType

    from pydantic import model_validator

    class InnerWrap(RecipeInputBase):
        b: str = "y"

        @model_validator(mode="wrap")
        @classmethod
        def _wrap(cls, data: AnyType, handler) -> AnyType:
            return data  # handler deliberately not invoked

    if placement == "nested":

        class Model(RecipeInputBase):
            leaf: InnerWrap = InnerWrap()

    elif placement == "two_deep":

        class Middle(RecipeInputBase):
            leaf: InnerWrap = InnerWrap()

        class Model(RecipeInputBase):
            holder: Middle = Middle()

    elif placement == "in_list":

        class Model(RecipeInputBase):
            items: ListType[InnerWrap] = []

    elif placement == "in_dict":

        class Model(RecipeInputBase):
            mapping: DictType[str, InnerWrap] = {}

    elif placement == "optional":

        class Model(RecipeInputBase):
            maybe: OptionalType[InnerWrap] = None

    else:
        # A union parks the wrap in ``choices[1]`` — a LIST position. Every other
        # placement above reaches it through dicts only, so without this case the
        # walk's list descent is unexercised and can be deleted with the suite
        # green (issue #145).
        from typing import Union as UnionType

        class Plain(RecipeInputBase):
            c: str = "z"

        class Model(RecipeInputBase):
            u: UnionType[Plain, InnerWrap] = Plain()

    # The ROOT config is impeccable — which is why a spine walk accepted it.
    assert Model.__pydantic_core_schema__["config"]["extra_fields_behavior"] == "forbid"
    with pytest.raises(ValueError, match="mode='wrap' or mode='plain'"):
        build_test_registry((_reg(input_model=Model),))


def test_the_nested_wrap_hole_is_invisible_to_the_schema_gate():
    """Why the extras gate has to carry this one alone.

    The nested model is genuinely declared ``extra="forbid"``, so its published
    schema says ``additionalProperties: false`` and the schema walk has nothing
    to object to. Only the compiled schema shows that a wrapper sits over its
    validation.
    """
    from typing import Any as AnyType

    from pydantic import model_validator

    class InnerWrap(RecipeInputBase):
        b: str = "y"

        @model_validator(mode="wrap")
        @classmethod
        def _wrap(cls, data: AnyType, handler) -> AnyType:
            return data

    class Model(RecipeInputBase):
        leaf: InnerWrap = InnerWrap()

    # The schema gate passes...
    registry_module._check_input_schema_closed("probe", Model)
    # ...and the leak is real: the nested field is a raw mapping, not a model.
    leaked = Model.model_validate({"leaf": {"b": "y", "smuggled": "v"}}).leaf
    assert isinstance(leaked, dict)
    assert leaked["smuggled"] == "v"
    # ...so only the extras gate stands between this and an executor.
    with pytest.raises(ValueError, match="mode='wrap' or mode='plain'"):
        registry_module._check_input_model_forbids_extras("probe", Model)


@pytest.mark.parametrize("mode", ["wrap", "plain"])
def test_a_field_validator_of_a_bypass_capable_mode_is_also_rejected(mode):
    """The rule is "is there a wrapper", not "does this wrapper reach a model".

    An earlier version rejected only wrappers sitting OVER A MODEL, so that an
    ordinary ``field_validator(mode="wrap")`` over a ``str`` would still be
    allowed. That classification is not decidable from the schema, and four
    separate shapes defeated it — a ``function-plain`` with no ``schema`` key at
    all, a wrapper whose immediate child is ``nullable``/``list``/``dict``/union
    rather than the model beneath it, an expiring hop budget that answered "not a
    model" instead of "cannot tell", and a container-key collision that pruned
    whole subtrees. "Is there a wrapper" is a lookup; "does it reach a model" is
    program analysis (issue #145, Codex review).

    The cost is real but small and measured: no production input model uses
    either mode, and a recipe input model is already frozen, closed and bounded.
    """
    from pydantic import field_validator

    if mode == "wrap":

        class Model(RecipeInputBase):
            a: str = "x"

            @field_validator("a", mode="wrap")
            @classmethod
            def _check(cls, value, handler):
                return handler(value)

    else:

        class Model(RecipeInputBase):
            a: str = "x"

            @field_validator("a", mode="plain")
            @classmethod
            def _check(cls, value):
                return str(value)

    # The EXTRAS gate is the one asserting the mode rule...
    with pytest.raises(ValueError, match="mode='wrap' or mode='plain'"):
        registry_module._check_input_model_forbids_extras("probe", Model)
    # ...and registration fails either way. For mode="plain" the SCHEMA gate
    # happens to reject first, because a plain validator erases the field's
    # published type and the field becomes unconstrained — a different rule
    # catching the same model, which is what two independent gates are for.
    with pytest.raises(ValueError):
        build_test_registry((_reg(input_model=Model),))


@pytest.mark.parametrize("field_name", ["config", "metadata", "default", "serialization"])
def test_a_field_named_like_an_excluded_key_is_still_walked(field_name):
    """The exclusion list applies to SCHEMA NODES, not to user-keyed containers.

    ``fields`` maps a field's name to its node, so filtering those same strings
    there pruned the entire subtree of any field called ``config``, ``metadata``
    or ``default`` — and a handler-skipping validator under such a field was
    never seen at all. A node is identified by having a string ``type``;
    anything else is a container and every value in it is walked
    (issue #145, Codex review).
    """
    from typing import Any as AnyType

    from pydantic import create_model, model_validator

    class InnerWrap(RecipeInputBase):
        b: str = "y"

        @model_validator(mode="wrap")
        @classmethod
        def _wrap(cls, data: AnyType, handler) -> AnyType:
            return data

    Model = create_model(
        "Colliding", __base__=RecipeInputBase, **{field_name: (InnerWrap, InnerWrap())}
    )
    with pytest.raises(ValueError, match="mode='wrap' or mode='plain'"):
        build_test_registry((_reg(input_model=Model),))


@pytest.mark.parametrize(
    "shape", ["plain_without_schema_key", "wrap_over_optional_model"]
)
def test_a_wrapper_that_defeats_target_classification_is_still_rejected(shape):
    """The two shapes that broke "does this wrapper reach a model".

    ``field_validator(mode="plain", json_schema_input_type=...)`` emits a
    ``function-plain`` node carrying ``json_schema_input_schema`` and NO
    ``schema`` key, so a target lookup found nothing and concluded "not a model".
    A wrapper over ``Optional[Model]`` has ``nullable`` as its immediate child
    for the same reason. Both leaked the caller's raw mapping
    (issue #145, Codex review).
    """
    from typing import Any as AnyType
    from typing import Optional as OptionalType

    from pydantic import BaseModel, ConfigDict, field_validator

    class ClosedLeaf(BaseModel):
        model_config = ConfigDict(extra="forbid")

        n: str = "x"

    if shape == "plain_without_schema_key":

        class Model(RecipeInputBase):
            leaf: AnyType = None

            @field_validator("leaf", mode="plain", json_schema_input_type=ClosedLeaf)
            @classmethod
            def _check(cls, value):
                return value

    else:

        class Model(RecipeInputBase):
            maybe: OptionalType[ClosedLeaf] = None

            @field_validator("maybe", mode="wrap")
            @classmethod
            def _check(cls, value, handler):
                return value  # handler skipped

    with pytest.raises(ValueError, match="mode='wrap' or mode='plain'"):
        build_test_registry((_reg(input_model=Model),))


@pytest.mark.parametrize("colliding_name", ["type", "schema", "config", "definitions"])
def test_a_field_named_like_a_schema_keyword_does_not_break_the_core_walk(
    colliding_name,
):
    """The core-schema walk descends through CONTAINERS keyed by field NAME.

    ``fields`` maps a field's name to its node, so a model with a field called
    ``type`` makes ``container["type"]`` a dict — and a membership test on a dict
    raises ``TypeError: unhashable type``. The same defect the JSON-schema walk
    was fixed for one gate over, made a second time in a second walk: a schema
    document contains DATA and CONTAINERS as well as nodes (issue #145).
    """
    from pydantic import create_model

    Model = create_model(
        "Colliding", __base__=RecipeInputBase, **{colliding_name: (str, "x")}
    )
    registry_module._check_input_model_forbids_extras("probe", Model)
    build_test_registry((_reg(input_model=Model),))


def test_a_default_value_that_looks_like_a_schema_is_not_walked():
    """``default`` holds a caller VALUE, not schema.

    A ``{"type": "default", "schema": {...}, "default": <value>}`` node carries
    whatever the author wrote, and walking it as schema crashed this gate on a
    default containing a ``type`` key.
    """
    from pydantic import BaseModel, ConfigDict

    class Leaf(BaseModel):
        model_config = ConfigDict(extra="forbid")

        type: str = "object"  # a VALUE that looks like a node kind

    class Model(RecipeInputBase):
        leaf: Leaf = Leaf()

    registry_module._check_input_model_forbids_extras("probe", Model)
    build_test_registry((_reg(input_model=Model),))


def test_a_default_value_naming_a_banned_node_type_is_not_a_rejection():
    """Excluding ``default`` from the walk is load-bearing, not decoration.

    Now that a bare ``function-wrap`` anywhere is rejected outright, walking DATA
    would read a caller's default value as a node — so an ordinary closed field
    whose default happens to contain ``{"type": "function-wrap"}`` would be
    refused. The string is a value, not a node kind (issue #145).
    """
    from typing import Dict as DictType

    class Model(RecipeInputBase):
        settings: DictType[str, str] = {"type": "function-wrap"}

    registry_module._check_input_model_forbids_extras("probe", Model)
    build_test_registry((_reg(input_model=Model),))


def test_mutually_recursive_input_models_resolve():
    """Two models referring to each other still resolve through ``definitions``."""
    from typing import Optional as OptionalType

    class MutA(RecipeInputBase):
        a: str = "x"
        b: OptionalType["MutB"] = None

    class MutB(RecipeInputBase):
        b: str = "y"
        a: OptionalType["MutA"] = None

    MutA.model_rebuild()
    MutB.model_rebuild()

    registry_module._check_input_model_forbids_extras("probe", MutA)
    registry_module._check_input_model_forbids_extras("probe", MutB)


@pytest.mark.parametrize("mode", ["before", "after"])
def test_passive_wrappers_are_not_rejected(mode):
    """``before`` and ``after`` cannot skip model validation, so they must pass.

    Without this the wrap rule could be "reject every wrapper", which would
    false-reject ``ComposeDbRestFanoutInputV1`` — a production model carrying a
    ``mode="after"`` validator.
    """
    from typing import Any as AnyType

    from pydantic import model_validator

    if mode == "before":

        class Model(RecipeInputBase):
            a: str = "x"

            @model_validator(mode="before")
            @classmethod
            def _hook(cls, data: AnyType) -> AnyType:
                return data

    else:

        class Model(RecipeInputBase):
            a: str = "x"

            @model_validator(mode="after")
            def _hook(self):
                return self

    # Model validation still runs, so extras are still rejected.
    with pytest.raises(Exception):
        Model.model_validate({"a": "x", "smuggled": "v"})
    registry_module._check_input_model_forbids_extras("probe", Model)  # accepts


def test_a_recursive_input_model_is_resolved_not_refused():
    """A recursive model parks itself in ``definitions`` behind a ref.

    The top-level node is ``definitions`` and its ``schema`` is a
    ``definition-ref``; following ``schema`` alone dead-ends at the reference, and
    fail-closed then rejects a perfectly closed model. Resolving the ref is what
    keeps fail-closed from becoming fail-wrong (issue #145, Codex review).
    """
    from typing import Optional as OptionalType

    class Recursive(RecipeInputBase):
        name: str = "n"
        child: OptionalType["Recursive"] = None

    Recursive.model_rebuild()

    core = Recursive.__pydantic_core_schema__
    assert core["type"] == "definitions"
    assert core["schema"]["type"] == "definition-ref"  # the dead end
    # It genuinely is closed...
    with pytest.raises(Exception):
        Recursive.model_validate({"name": "n", "smuggled": "v"})
    # ...so it must register.
    registry_module._check_input_model_forbids_extras("probe", Recursive)


def test_a_recursive_input_model_that_allows_extras_is_still_rejected():
    """Resolving the ref must not become a way of skipping the check."""
    from typing import Optional as OptionalType

    from pydantic import ConfigDict

    class RecursiveOpen(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)

        name: str = "n"
        child: OptionalType["RecursiveOpen"] = None

    RecursiveOpen.model_rebuild()

    assert RecursiveOpen.__pydantic_core_schema__["type"] == "definitions"
    with pytest.raises(ValueError, match="must reject undeclared keys"):
        registry_module._check_input_model_forbids_extras("probe", RecursiveOpen)


def test_an_unreadable_core_schema_fails_closed():
    """"Learned nothing" must fail the build, not pass it.

    The core schema is a private-ish attribute. If a future pydantic moves or
    renames it, this gate must break loudly rather than silently start accepting
    every model — a guard that fails open on an upgrade is worse than no guard,
    because nothing announces the change.
    """
    from unittest.mock import patch

    class Opaque(RecipeInputBase):
        process_key: str = "p"

    with patch.object(Opaque, "__pydantic_core_schema__", {"type": "nonsense"}):
        with pytest.raises(ValueError, match="could not be determined"):
            registry_module._check_input_model_forbids_extras("probe", Opaque)


def test_the_extras_gate_accepts_a_model_with_required_fields():
    """The probe needs no VALID payload.

    A model with a required field still reports ``extra_forbidden`` alongside
    its missing-field error, which is what makes running the validator usable as
    a registration-time gate rather than only a runtime one.
    """

    class Required(RecipeInputBase):
        needed: str

    registry_module._check_input_model_forbids_extras("probe", Required)  # no raise


def test_a_bounded_mapping_is_not_mistaken_for_an_open_object():
    """``Dict[str, str]`` renders as ``additionalProperties: {"type": "string"}``.

    That is a SCHEMA, not ``true`` — the mapping is bounded and must register.
    Without this control the closedness rule could pass every rejection test
    above by simply refusing everything.

    Deliberately exercises every member of ``_CONSTRAINING_KEYWORDS`` that a real
    model can produce — ``type``, ``$ref``, ``anyOf``, ``oneOf``, ``enum``,
    ``const``, ``properties``, ``items``, ``prefixItems``,
    ``additionalProperties`` — but NOT ``not``, which is deliberately absent from
    the set because it does not bound anything. Dropping a keyword makes the check
    STRICTER, so the failure mode is a spurious rejection of a legitimate model;
    only a bounded shape using that keyword can observe it, and nine of the
    twelve had no such shape here (issue #145, live QA).
    """
    from typing import Dict as DictType
    from typing import List as ListType
    from typing import Literal as LiteralType
    from typing import Optional as OptionalType
    from typing import Tuple as TupleType
    from typing import Union as UnionType

    from pydantic import BaseModel, ConfigDict

    class ClosedLeaf(BaseModel):
        model_config = ConfigDict(extra="forbid")

        name: str = "x"

    class VariantA(BaseModel):
        model_config = ConfigDict(extra="forbid")

        kind: LiteralType["a"] = "a"
        a: int = 0

    class VariantB(BaseModel):
        model_config = ConfigDict(extra="forbid")

        kind: LiteralType["b"] = "b"
        b: int = 0

    class Bounded(RecipeInputBase):
        process_key: str = "p"
        leaf: ClosedLeaf = ClosedLeaf()          # $ref -> $defs
        names: ListType[str] = []                # items
        lookup: DictType[str, str] = {}          # additionalProperties as a schema
        nested_map: DictType[str, ClosedLeaf] = {}
        maybe: OptionalType[str] = None          # anyOf
        choice: LiteralType["x", "y"] = "x"      # enum
        pinned: LiteralType["only"] = "only"     # const
        pair: TupleType[str, int] = ("a", 1)     # prefixItems
        variant: UnionType[VariantA, VariantB] = VariantA()

    registry = build_test_registry((_reg(input_model=Bounded),))
    assert registry.resolve("test.recipe").input_schema_id.endswith("Bounded")


def test_every_production_input_model_is_closed():
    """The check is not vacuous in the direction that matters: it must ACCEPT
    the models actually shipped, or it would be a build break rather than a
    guard."""
    registry = production_registry()
    closed = [
        d.recipe_id for d in registry.descriptors() if d.input_schema_id is not None
    ]
    assert closed  # some production entry does declare an input model
    # Constructed above without raising — the assertion is that this line runs.


# ---------------------------------------------------------------------------
# Executor shape
# ---------------------------------------------------------------------------


async def _async_executor(inp):  # pragma: no cover - rejected at registration
    return ()


def test_a_coroutine_executor_is_rejected():
    with pytest.raises(ValueError, match="coroutine"):
        build_test_registry((_reg(executor=_async_executor),))


def test_a_lambda_executor_is_rejected():
    with pytest.raises(ValueError):
        build_test_registry((_reg(executor=lambda inp: ()),))


def test_a_partial_executor_is_rejected():
    import functools

    with pytest.raises(ValueError, match="module-level function"):
        build_test_registry(
            (_reg(executor=functools.partial(emit_api_to_api_sync)),)
        )


def test_a_closure_executor_is_rejected():
    """A closure's captured state is invisible to ``inspect.getsource``.

    Its implementation hash would therefore be blind to the very thing that
    changes its behavior — and the skew report would say ``match`` for two
    registries running different code.
    """
    captured = {"n": 1}

    def closing(inp):  # pragma: no cover - rejected at registration
        return captured["n"]

    with pytest.raises(ValueError):
        build_test_registry((_reg(executor=closing),))


def test_a_bound_method_executor_is_rejected():
    class Holder:
        def run(self, inp):  # pragma: no cover - rejected at registration
            return ()

    with pytest.raises(ValueError):
        build_test_registry((_reg(executor=Holder().run),))


# ---------------------------------------------------------------------------
# Deterministic discovery
# ---------------------------------------------------------------------------


def test_registry_revision_is_invariant_under_registration_order():
    """Randomized, because a single reversed pair proves much less."""
    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).registry_revision
    rng = random.Random(20261010)
    for _ in range(8):
        shuffled = list(PRODUCTION_REGISTRATIONS)
        rng.shuffle(shuffled)
        assert build_test_registry(tuple(shuffled)).registry_revision == baseline


def test_descriptors_are_sorted_by_id_then_parsed_semver_then_kind():
    registry = build_test_registry(
        (
            _reg(recipe_id="b.recipe", recipe_version="1.9.0"),
            _reg(recipe_id="a.recipe", recipe_version="1.10.0"),
            _reg(recipe_id="a.recipe", recipe_version="1.9.0", is_default=False),
        )
    )
    assert [
        (d.recipe_id, d.recipe_version) for d in registry.descriptors()
    ] == [("a.recipe", "1.9.0"), ("a.recipe", "1.10.0"), ("b.recipe", "1.9.0")]


def test_a_descriptor_change_moves_the_registry_revision():
    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).registry_revision
    mutated = list(PRODUCTION_REGISTRATIONS) + [
        _reg(recipe_id="extra.recipe", recipe_version="1.0.0")
    ]
    assert build_test_registry(tuple(mutated)).registry_revision != baseline


def test_the_registry_module_exposes_no_runtime_registration_api():
    """"There is no runtime registrar" is asserted, not merely documented."""
    for name in ("register", "add", "install", "unregister", "clear"):
        assert not hasattr(registry_module, name), name
        assert not hasattr(registry_module.RecipeRegistry, name), name
    from boomi_mcp import recipes

    for name in ("register", "register_recipe", "add_recipe"):
        assert not hasattr(recipes, name), name


def test_the_production_registrations_are_an_immutable_tuple():
    assert isinstance(PRODUCTION_REGISTRATIONS, tuple)
    with pytest.raises((AttributeError, TypeError)):
        PRODUCTION_REGISTRATIONS.append(_reg())  # type: ignore[attr-defined]


def test_the_builtins_package_does_no_package_scanning():
    """A scan would make the registry a property of the filesystem."""
    import boomi_mcp.recipes.builtins as builtins_pkg

    source = Path(builtins_pkg.__file__).read_text()
    for token in ("pkgutil", "importlib", "walk_packages", "iter_modules"):
        assert token not in source, token


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------


def test_provenance_is_derived_from_the_registered_callable():
    registry = build_test_registry((_reg(),))
    provenance = registry.resolve("test.recipe").provenance
    assert provenance.package_name == "boomi_mcp"
    assert provenance.module == emit_api_to_api_sync.__module__
    assert provenance.symbol == emit_api_to_api_sync.__qualname__
    assert len(provenance.implementation_sha256) == 64
    assert len(provenance.descriptor_sha256) == 64


def test_a_different_executor_yields_a_different_implementation_hash():
    from boomi_mcp.recipes.builtins.fanout import (
        ComposeDbRestFanoutInputV1,
        emit_db_rest_fanout,
    )

    a = build_test_registry((_reg(),)).resolve("test.recipe")
    b = build_test_registry(
        (_reg(executor=emit_db_rest_fanout, input_model=ComposeDbRestFanoutInputV1),)
    ).resolve("test.recipe")
    assert a.provenance.implementation_sha256 != b.provenance.implementation_sha256


def test_the_same_registration_yields_the_same_implementation_hash():
    a = build_test_registry((_reg(),)).resolve("test.recipe")
    b = build_test_registry((_reg(),)).resolve("test.recipe")
    assert a.provenance.implementation_sha256 == b.provenance.implementation_sha256
    assert a.provenance.descriptor_sha256 == b.provenance.descriptor_sha256


def test_the_descriptor_hash_excludes_itself():
    """A self-referential hash is unverifiable — a reader could not reproduce it."""
    descriptor = build_test_registry((_reg(),)).resolve("test.recipe")
    assert descriptor.provenance.descriptor_sha256 not in (
        descriptor.provenance.implementation_sha256,
    )
    # Reproducible from the published body: same registration, same hash.
    assert (
        build_test_registry((_reg(),)).resolve("test.recipe").provenance.descriptor_sha256
        == descriptor.provenance.descriptor_sha256
    )


def test_the_descriptor_hash_covers_derived_provenance():
    """Excluding derived provenance made the hash blind to the skew it reports.

    ``implementation_sha256`` covers the defining module's source, and nothing
    else in the body moves when the same source ships from a different commit —
    so with ``source_revision`` outside the hashed body, two registries built
    from different builds of identical recipe source produced identical
    ``descriptor_sha256`` values and compared as "same descriptor". The plan
    specifies the descriptor hash over the body WITH derived provenance,
    excluding only the self-referential field (issue #145, §6 architect review).
    """
    def _build(revision, monkeypatch):
        monkeypatch.setattr(registry_module, "source_revision", lambda _mods: revision)
        return build_test_registry((_reg(),)).resolve("test.recipe")

    with pytest.MonkeyPatch.context() as mp:
        a = _build("a" * 40, mp)
    with pytest.MonkeyPatch.context() as mp:
        b = _build("b" * 40, mp)
    assert a.provenance.source_revision != b.provenance.source_revision
    # Same recipe source, so the implementation hash is unmoved...
    assert a.provenance.implementation_sha256 == b.provenance.implementation_sha256
    # ...and only the descriptor hash can carry the difference.
    assert a.provenance.descriptor_sha256 != b.provenance.descriptor_sha256


def test_the_descriptor_hash_covers_the_package_version():
    """The second derived field, pinned separately.

    The ``source_revision`` test alone left this unobserved — removing
    ``provenance_package_version`` from the hashed body kept the whole suite
    green — even though the motivating example is precisely "the same recipe
    source shipped from a different package version" (issue #145, live QA).

    ``provenance_package_name`` gets no such test on purpose: it is
    ``Literal["boomi_mcp"]`` on ``RecipeProvenanceV1``, so it cannot vary and
    contributes a constant to every body. It is in the hash for completeness of
    the record, never to discriminate.
    """
    def _build(version, monkeypatch):
        monkeypatch.setattr(registry_module, "package_version", lambda: version)
        return build_test_registry((_reg(),)).resolve("test.recipe")

    with pytest.MonkeyPatch.context() as mp:
        a = _build("1.0.0", mp)
    with pytest.MonkeyPatch.context() as mp:
        b = _build("9.9.9", mp)
    assert a.provenance.package_version != b.provenance.package_version
    assert a.provenance.implementation_sha256 == b.provenance.implementation_sha256
    assert a.provenance.descriptor_sha256 != b.provenance.descriptor_sha256


def test_the_recipes_package_surface_excludes_the_test_only_factory():
    """"No runtime/public registration API" is a claim about the SURFACE.

    Re-exporting ``build_test_registry`` beside ``production_registry`` made a
    test-only factory read as a supported way to assemble a registry at runtime,
    and re-adding it left the suite green (issue #145, live QA).
    """
    import boomi_mcp.recipes as package

    assert "build_test_registry" not in package.__all__
    assert not hasattr(package, "build_test_registry")
    # ...and it is still reachable where a test-only entry point belongs.
    from boomi_mcp.recipes.registry import build_test_registry as factory

    assert callable(factory)


def test_declaration_order_does_not_move_the_descriptor_hash():
    """Prerequisites are a SET of conditions, so their order carries no meaning.

    Carried through verbatim, two registrations declaring the same requirements
    in a different order hashed differently and a skew comparison reported a
    mismatch between two registries running identical code (issue #145, §6
    architect review).
    """
    first = RecipeCapabilityRequirementV1(
        authority="process_ir",
        subject="generalized_connector_call",
        required_state="supported",
    )
    second = RecipeCapabilityRequirementV1(
        authority="process_ir",
        subject="mixed_connector_execution",
        required_state="supported",
    )
    forward = build_test_registry(
        (_reg(capability_requirements=(first, second)),)
    ).resolve("test.recipe")
    backward = build_test_registry(
        (_reg(capability_requirements=(second, first)),)
    ).resolve("test.recipe")
    assert (
        forward.provenance.descriptor_sha256 == backward.provenance.descriptor_sha256
    )
    # Canonicalized on the descriptor itself, not only inside the hash — a
    # reader of the published descriptor sees the same order the hash saw.
    assert forward.capability_requirements == backward.capability_requirements


def test_a_duplicate_declaration_collapses():
    """An exactly-repeated requirement is one requirement, and must hash as one."""
    requirement = RecipeCapabilityRequirementV1(
        authority="process_ir",
        subject="generalized_connector_call",
        required_state="supported",
    )
    once = build_test_registry(
        (_reg(capability_requirements=(requirement,)),)
    ).resolve("test.recipe")
    twice = build_test_registry(
        (_reg(capability_requirements=(requirement, requirement)),)
    ).resolve("test.recipe")
    assert len(twice.capability_requirements) == 1
    assert once.provenance.descriptor_sha256 == twice.provenance.descriptor_sha256


def test_source_revision_prefers_the_image_file(tmp_path):
    revision = "a" * 40
    path = tmp_path / "BUILD_REVISION"
    path.write_text(revision)
    assert image_build_revision(str(path)) == revision
    assert source_revision(["boomi_mcp.build_info"], path=str(path)) == revision


def test_a_malformed_image_revision_is_no_evidence_at_all(tmp_path):
    """Half-read is not weaker evidence; reporting it would be confidently wrong."""
    for bad in ("", "not-hex", "ZZZ", "a" * 41, "abc"):
        path = tmp_path / "BUILD_REVISION"
        path.write_text(bad)
        assert image_build_revision(str(path)) is None


def test_the_local_fallback_is_a_labelled_source_digest():
    revision = source_revision(["boomi_mcp.build_info"], path="/nonexistent/path")
    assert is_source_digest(revision)
    assert revision.startswith("source-sha256:")


def test_the_source_digest_is_order_independent_and_content_sensitive():
    a = source_digest(["boomi_mcp.build_info", "boomi_mcp.errors"])
    b = source_digest(["boomi_mcp.errors", "boomi_mcp.build_info"])
    assert a == b
    assert a != source_digest(["boomi_mcp.errors"])


def test_the_source_digest_fails_closed_on_an_unreadable_module():
    with pytest.raises(Exception):
        source_digest(["boomi_mcp.definitely_not_a_module"])


# ---------------------------------------------------------------------------
# Capability preflight
# ---------------------------------------------------------------------------


def test_an_absent_capability_subject_fails_at_construction():
    """A typo must not sit dormant and later blame the caller's platform."""
    with pytest.raises(ValueError, match="unknown"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority="process_emitter",
                            subject="no.such.emitter",
                            required_state="supported",
                        ),
                    )
                ),
            )
        )


def test_a_gated_process_ir_capability_is_refused_at_preflight():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="process_ir", subject="joins", required_state="supported"
                    ),
                )
            ),
        )
    )
    with pytest.raises(RecipeError) as exc:
        registry.preflight_capabilities(registry.resolve("test.recipe"))
    assert exc.value.diagnostics[0].code == RECIPE_CAPABILITY_GATED


def test_a_supported_process_ir_capability_passes_preflight():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="process_ir",
                        subject="rich_branch_decision_bodies",
                        required_state="supported",
                    ),
                )
            ),
        )
    )
    registry.preflight_capabilities(registry.resolve("test.recipe"))


def test_plannable_only_is_satisfied_by_the_stronger_emittable_state():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="system_topology",
                        subject="process",
                        required_state="plannable-only",
                    ),
                )
            ),
        )
    )
    registry.preflight_capabilities(registry.resolve("test.recipe"))


def test_a_gated_topology_subject_never_satisfies_a_requirement():
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="system_topology",
                        subject="queue_reference",
                        required_state="plannable-only",
                    ),
                )
            ),
        )
    )
    with pytest.raises(RecipeError):
        registry.preflight_capabilities(registry.resolve("test.recipe"))


# ---------------------------------------------------------------------------
# Skew
# ---------------------------------------------------------------------------


def _expected_from(registry, **overrides):
    entries = tuple(
        ExpectedRecipeEntryV1(
            recipe_id=d.recipe_id,
            recipe_version=d.recipe_version,
            implementation_sha256=d.provenance.implementation_sha256,
        )
        for d in registry.descriptors()
    )
    payload = dict(
        registry_revision=registry.registry_revision,
        source_revision=registry.source_revision_value,
        entries=entries,
    )
    payload.update(overrides)
    return ExpectedRecipeRegistryV1(**payload)


def test_no_expectation_is_reported_as_not_requested():
    assert production_registry().compare(None).status == "not_requested"


def test_an_exact_expectation_matches():
    registry = production_registry()
    assert registry.compare(_expected_from(registry)).status == "match"


def test_a_missing_recipe_is_a_mismatch():
    registry = production_registry()
    expected = _expected_from(
        registry,
        entries=(
            ExpectedRecipeEntryV1(recipe_id="not.registered", recipe_version="1.0.0"),
        ),
    )
    skew = registry.compare(expected)
    assert skew.status == "mismatch"
    assert skew.missing_from_live == ("not.registered",)


def test_equal_versions_with_different_code_are_a_mismatch_not_a_match():
    """The skew this issue actually exists for.

    Two registries can agree on ``api_to_api_sync@0.1.0`` and run different
    bytes. A version-only comparison would call that a match.
    """
    registry = production_registry()
    entries = []
    for d in registry.descriptors():
        sha = d.provenance.implementation_sha256
        if d.recipe_id == RECIPE_API_TO_API_SYNC:
            sha = "0" * 64
        entries.append(
            ExpectedRecipeEntryV1(
                recipe_id=d.recipe_id,
                recipe_version=d.recipe_version,
                implementation_sha256=sha,
            )
        )
    skew = registry.compare(_expected_from(registry, entries=tuple(entries)))
    assert skew.status == "mismatch"
    assert [m.recipe_id for m in skew.implementation_mismatches] == [
        RECIPE_API_TO_API_SYNC
    ]
    assert skew.version_mismatches == ()


def test_a_version_mismatch_is_reported_separately():
    registry = production_registry()
    expected = _expected_from(
        registry,
        entries=(
            ExpectedRecipeEntryV1(
                recipe_id=RECIPE_DB_REST_FANOUT, recipe_version="9.9.9"
            ),
        ),
    )
    skew = registry.compare(expected)
    assert skew.status == "mismatch"
    assert [m.recipe_id for m in skew.version_mismatches] == [RECIPE_DB_REST_FANOUT]


def test_a_registry_revision_difference_is_reported():
    registry = production_registry()
    skew = registry.compare(_expected_from(registry, registry_revision="f" * 64))
    assert skew.status == "mismatch"
    assert skew.registry_revision_mismatch is True


def test_a_source_revision_difference_is_reported():
    registry = production_registry()
    skew = registry.compare(_expected_from(registry, source_revision="deadbeef"))
    assert skew.status == "mismatch"
    assert skew.source_revision_mismatch is True


def test_every_skew_collection_is_sorted():
    registry = production_registry()
    expected = _expected_from(
        registry,
        entries=(
            ExpectedRecipeEntryV1(recipe_id="z.missing", recipe_version="1.0.0"),
            ExpectedRecipeEntryV1(recipe_id="a.missing", recipe_version="1.0.0"),
        ),
    )
    skew = registry.compare(expected)
    assert list(skew.missing_from_live) == sorted(skew.missing_from_live)
    assert list(skew.live_only) == sorted(skew.live_only)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


def test_the_snapshot_is_fully_sorted_and_carries_the_revisions():
    snapshot = production_registry().snapshot()
    ids = [entry["recipe_id"] for entry in snapshot["entries"]]
    assert ids == sorted(ids)
    assert len(snapshot["registry_revision"]) == 64
    assert snapshot["source_revision"]
    assert list(snapshot["capability_revisions"]) == sorted(
        snapshot["capability_revisions"]
    )


def test_the_snapshot_names_every_entry_kind_the_registry_distinguishes():
    kinds = {entry["entry_kind"] for entry in production_registry().snapshot()["entries"]}
    assert kinds == {
        "executable_recipe",
        "constraint_only",
        "advisory",
        "compatibility_adapter",
    }


def test_the_public_descriptor_payload_redacts_capability_subjects():
    """Capability subjects name DARK compiler-authority keys.

    ``tests/test_process_ir_compiler_surface.py`` forbids those on any
    LLM-visible surface, so the public view reports how many were checked
    without naming them. They are still enforced — see the preflight tests above.
    """
    descriptor = production_registry().resolve(RECIPE_DB_REST_FANOUT)
    assert descriptor.capability_requirements  # really has some
    payload = descriptor.public_payload()
    assert payload["capability_requirements"]["count"] == len(
        descriptor.capability_requirements
    )
    import json as _json

    blob = _json.dumps(payload)
    for dark in ("connectoraction_source", "connectoraction_target", "doccacheload"):
        assert dark not in blob, dark


# ---------------------------------------------------------------------------
# Doctrine is not executable
# ---------------------------------------------------------------------------


def test_doctrine_prose_never_resolves_to_a_recipe(monkeypatch):
    """Prose that LOOKS like a recipe id executes nothing.

    Spies on every registered executor, so "no execution" is observed rather
    than inferred from an absent exception.
    """
    calls = []
    for name in ("emit_api_to_api_sync", "emit_api_to_database_sync"):
        original = getattr(catalog, name, None)
        if original is None:
            continue

        def spy(inp, _name=name):  # pragma: no cover - must never run
            calls.append(_name)
            return ()

        monkeypatch.setattr(catalog, name, spy, raising=False)

    registry = production_registry()
    for prose in (
        "Use the api_to_api_sync archetype for REST-to-REST replication.",
        "recipe: db_rest_fanout",
        "design_doctrine:connector_retry_design",
        "api_to_api_sync",  # the ARCHETYPE name — not a recipe id
        "Consider boomi.archetype.api_to_api_sync when the source is REST.",
    ):
        with pytest.raises(RecipeError):
            registry.resolve(prose)
    assert calls == []


def test_naming_a_real_recipe_id_resolves_but_still_executes_nothing(monkeypatch):
    """Resolution is LOOKUP, not execution — the two are separate steps.

    A caller may name a registered recipe; that returns a descriptor and runs no
    code. Execution needs the engine, a validated input, and a materialization
    catalog. Pinned so "resolve" can never quietly become "run".
    """
    calls = []
    monkeypatch.setattr(
        catalog,
        "emit_api_to_api_sync",
        lambda inp: calls.append("ran") or (),  # pragma: no cover
        raising=False,
    )
    descriptor = production_registry().resolve(RECIPE_API_TO_API_SYNC)
    assert descriptor.entry_kind == "executable_recipe"
    assert calls == []


def test_the_advisory_entry_has_no_executor_to_call():
    registry = production_registry()
    descriptor = registry.resolve(RECIPE_ADVISORY_INTEGRATION_DESIGN)
    assert descriptor.entry_kind == "advisory"
    assert descriptor.output_types == ()
    assert descriptor.input_schema_id is None
    with pytest.raises(RecipeError):
        registry.executor_for(descriptor)
    with pytest.raises(RecipeError):
        registry.input_model_for(descriptor)


def test_the_constraint_only_entry_emits_only_requirements():
    descriptor = production_registry().resolve(RECIPE_CONSTRAINT_INBOUND_VALIDATE)
    assert descriptor.entry_kind == "constraint_only"
    assert descriptor.output_types == ("constraint_requirement",)


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): the digest must track its CALLEES
# ---------------------------------------------------------------------------


def test_the_implementation_digest_covers_the_defining_module_not_only_the_symbol():
    """A behaviour-changing edit to a SHARED HELPER must move the hash.

    Live QA found this: both sync recipes delegate their whole body to
    ``_contributions``, so hashing only the registered function left every hash
    unmoved while ``build_from_archetype`` went from a working spec to a hard
    failure. A caller comparing implementation hashes would have been told
    ``match`` about a registry whose output had changed.

    Simulated by perturbing what ``inspect.getsource`` returns for the MODULE
    while the registered function's own source is untouched — which is exactly
    the shape of the real defect.
    """
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    baseline = build_test_registry((_reg(),)).resolve("test.recipe")

    real_getsource = inspect_module.getsource
    module_of_executor = inspect_module.getmodule(emit_api_to_api_sync)

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is module_of_executor:
            return text + "\n# a shared helper changed\n"
        return text

    original = registry_mod.inspect.getsource
    registry_mod.inspect.getsource = perturbed
    try:
        mutated = build_test_registry((_reg(),)).resolve("test.recipe")
    finally:
        registry_mod.inspect.getsource = original

    assert (
        mutated.provenance.implementation_sha256
        != baseline.provenance.implementation_sha256
    )


def test_two_recipes_sharing_a_module_stay_distinguishable():
    """Widening the digest must not collapse per-symbol attribution."""
    from boomi_mcp.recipes.builtins.sync import emit_api_to_database_sync

    a = build_test_registry(
        (_reg(recipe_id="r.a", executor=emit_api_to_api_sync),)
    ).resolve("r.a")
    b = build_test_registry(
        (_reg(recipe_id="r.b", executor=emit_api_to_database_sync),)
    ).resolve("r.b")
    assert a.provenance.implementation_sha256 != b.provenance.implementation_sha256


def test_a_recipe_in_another_module_is_unaffected_by_a_shared_helper_change():
    """The digest is per-MODULE, not per-package.

    Hashing the whole import closure would move every recipe's hash on any change
    anywhere — a hash of the package, not of a recipe. The fan-out recipe lives
    in its own module and must not move when the sync module does.
    """
    from boomi_mcp.recipes.builtins.fanout import (
        ComposeDbRestFanoutInputV1,
        emit_db_rest_fanout,
    )
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    def fanout_reg():
        return _reg(
            recipe_id="r.fanout",
            executor=emit_db_rest_fanout,
            input_model=ComposeDbRestFanoutInputV1,
        )

    baseline = build_test_registry((fanout_reg(),)).resolve("r.fanout")

    real_getsource = inspect_module.getsource
    sync_module = inspect_module.getmodule(emit_api_to_api_sync)

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is sync_module:
            return text + "\n# unrelated module changed\n"
        return text

    original = registry_mod.inspect.getsource
    registry_mod.inspect.getsource = perturbed
    try:
        after = build_test_registry((fanout_reg(),)).resolve("r.fanout")
    finally:
        registry_mod.inspect.getsource = original

    assert (
        after.provenance.implementation_sha256
        == baseline.provenance.implementation_sha256
    )


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): a partial comparison is reported as such
# ---------------------------------------------------------------------------


def _entries(registry, *, with_hashes):
    return tuple(
        ExpectedRecipeEntryV1(
            recipe_id=d.recipe_id,
            recipe_version=d.recipe_version,
            implementation_sha256=(
                d.provenance.implementation_sha256 if with_hashes else None
            ),
        )
        for d in registry.descriptors()
    )


def test_a_version_only_expectation_is_unknown_not_match():
    """Equal versions are not evidence of equal code — so do not say ``match``.

    A full entries list with no implementation hashes and no revisions is a
    VERSION comparison. Reporting it as ``match`` is the silent "looks fine"
    that ``RecipeRegistrySkewV1`` exists to forbid.
    """
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(entries=_entries(registry, with_hashes=False))
    )
    assert skew.status == "unknown"
    assert "partial_comparison" in skew.reason
    assert "not evidence of equal code" in skew.reason


def test_an_entries_expectation_with_full_hashes_is_a_real_match():
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(entries=_entries(registry, with_hashes=True))
    )
    assert skew.status == "match"
    assert skew.reason is None


def test_a_revision_alone_is_enough_for_a_real_match():
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            registry_revision=registry.registry_revision,
            entries=_entries(registry, with_hashes=False),
        )
    )
    assert skew.status == "match"


def test_a_source_revision_alone_is_enough_for_a_real_match():
    registry = production_registry()
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            source_revision=registry.source_revision_value,
            entries=_entries(registry, with_hashes=False),
        )
    )
    assert skew.status == "match"


def test_an_empty_entries_list_is_a_mismatch_not_unknown():
    """An empty entries list ASSERTS "this registry has no recipes".

    That is a claim, not an absent one — so the live registry's eight entries are
    a real difference, reported with ``live_only``. ``unknown`` is for a
    comparison that could not establish parity, not for one whose answer is no.
    """
    skew = production_registry().compare(ExpectedRecipeRegistryV1())
    assert skew.status == "mismatch"
    assert len(skew.live_only) == 8
    assert skew.missing_from_live == ()


def test_a_partial_comparison_that_finds_a_difference_is_still_a_mismatch():
    """A finding is a finding, however partial the comparison was."""
    registry = production_registry()
    entries = list(_entries(registry, with_hashes=False))
    entries[0] = ExpectedRecipeEntryV1(
        recipe_id=entries[0].recipe_id, recipe_version="9.9.9"
    )
    skew = registry.compare(ExpectedRecipeRegistryV1(entries=tuple(entries)))
    assert skew.status == "mismatch"
    assert skew.version_mismatches


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): the two hashes have DIFFERENT scopes
# ---------------------------------------------------------------------------
#
# ``implementation_sha256`` answers "WHICH recipe changed" and covers the entry's
# own defining module. ``source_revision`` answers "did anything in the layer
# change" and covers the whole execution path. Round 2 of live QA found the
# second claim documented before it was true: the digest listed only the executor
# modules, so an edit to ``engine.py`` or ``recipe_bridge.py`` moved nothing while
# both migrated presets went from a working spec to a hard failure.


def test_the_layer_module_list_covers_the_whole_recipe_package():
    """Pinned in BOTH directions against the package's real contents.

    A static list keeps the digest a property of the code rather than of the
    filesystem — but a static list that silently stops covering a new module is
    worse than a scan, so this is the guard that makes it safe.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    import boomi_mcp.recipes as recipes_pkg

    package_dir = Path(recipes_pkg.__file__).parent
    on_disk = set()
    for path in package_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue  # re-export shims carry no logic
        rel = path.relative_to(package_dir).with_suffix("")
        on_disk.add("boomi_mcp.recipes." + ".".join(rel.parts))

    listed = {m for m in RECIPE_LAYER_MODULES if m.startswith("boomi_mcp.recipes.")}
    assert listed == on_disk, listed ^ on_disk

    # Everything OUTSIDE the package must be claimed by clause 2 (a contract
    # module) or clause 3 (an engine invoker). Derived, not a literal set: a
    # hard-coded list would pass even if the RULE covered none of them, which is
    # exactly the gap live QA found (issue #145).
    outside = set(RECIPE_LAYER_MODULES) - listed
    contract_modules = {"boomi_mcp.build_info", "boomi_mcp.models.recipe_contributions"}
    unclaimed = outside - contract_modules - _engine_invoking_modules()
    assert unclaimed == set(), unclaimed
    # ...and neither clause is carrying a module the other already claims.
    assert contract_modules & _engine_invoking_modules() == set()


def test_the_contract_clause_covers_exactly_the_two_modules_it_names():
    """Clause 2 is a NAMED pair, not an open category.

    If it were open, "a contract module" would become the escape hatch the other
    two clauses exist to avoid. These two are outside ``recipes/`` only because
    ``models/`` may not import ``categories/`` and ``build_info`` must stay
    stdlib-only.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    for module in ("boomi_mcp.build_info", "boomi_mcp.models.recipe_contributions"):
        assert module in RECIPE_LAYER_MODULES, module
    # Neither is in the recipes package, and neither invokes the engine — which
    # is precisely why clause 2 has to exist.
    for module in ("boomi_mcp.build_info", "boomi_mcp.models.recipe_contributions"):
        assert not module.startswith("boomi_mcp.recipes."), module
        assert module not in _engine_invoking_modules(), module


def test_the_layer_module_list_is_sorted_and_unique():
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    assert list(RECIPE_LAYER_MODULES) == sorted(set(RECIPE_LAYER_MODULES))


@pytest.mark.parametrize(
    "module_name",
    [
        "boomi_mcp.recipes.engine",
        "boomi_mcp.recipes.composer",
        "boomi_mcp.recipes.materialization",
        "boomi_mcp.patterns.recipe_bridge",
        "boomi_mcp.models.recipe_contributions",
    ],
)
def test_source_revision_moves_for_any_module_in_the_execution_path(module_name):
    """The layer-wide backstop, tested where it previously did not reach.

    Each of these modules can change what a recipe run produces, and none of them
    is any single entry's defining module — so ``source_revision`` is the only
    thing that can notice.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.build_info as build_info_mod

    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value

    target = importlib.import_module(module_name)
    real_getsource = inspect_module.getsource

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is target:
            return text + "\n# changed\n"
        return text

    original = build_info_mod.inspect.getsource
    build_info_mod.inspect.getsource = perturbed
    try:
        mutated = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value
    finally:
        build_info_mod.inspect.getsource = original

    assert mutated != baseline, module_name


def test_an_executor_less_entry_hashes_its_declaration_source():
    """An adapter/advisory entry IS a declaration, so hash where it is written.

    Live QA found these hashing only ``(id, version, entry_kind)``, which never
    moved for ANY edit — including one to the catalog module they themselves
    declare. Four of eight published entries pinned nothing, in a field the skew
    note tells callers to rely on.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    def executor_less_hashes(registry):
        return {
            d.recipe_id: d.provenance.implementation_sha256
            for d in registry.descriptors()
            if d.entry_kind in ("compatibility_adapter", "advisory")
        }

    baseline = executor_less_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
    assert len(baseline) == 4

    catalog_module = importlib.import_module("boomi_mcp.recipes.builtins.catalog")
    real_getsource = inspect_module.getsource

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is catalog_module:
            return text + "\n# declaration changed\n"
        return text

    original = registry_mod.inspect.getsource
    registry_mod.inspect.getsource = perturbed
    try:
        mutated = executor_less_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
    finally:
        registry_mod.inspect.getsource = original

    for recipe_id, digest in baseline.items():
        assert mutated[recipe_id] != digest, recipe_id


def test_an_adapters_hash_depends_on_the_target_it_names():
    """Repointing an adapter at a different recipe must move its hash."""
    from boomi_mcp.recipes.contracts import RecipeReferenceV1

    def adapter(target_version):
        return RecipeRegistrationV1(
            recipe_id="test.adapter",
            recipe_version="1.0.0",
            entry_kind="compatibility_adapter",
            is_default=True,
            adapter_target=RecipeReferenceV1(
                recipe_id="test.target", recipe_version=target_version
            ),
        )

    # The targets must be REGISTERED — construction now rejects an adapter that
    # names one that is not, so the registry has to carry both versions.
    def target(version, default):
        return _reg(recipe_id="test.target", recipe_version=version, is_default=default)

    targets = (target("0.1.0", True), target("0.2.0", False))
    a = build_test_registry((adapter("0.1.0"), *targets)).resolve("test.adapter")
    b = build_test_registry((adapter("0.2.0"), *targets)).resolve("test.adapter")
    assert (
        a.provenance.implementation_sha256 != b.provenance.implementation_sha256
    )


def test_every_published_entry_pins_some_code():
    """No published entry may carry a hash that pins nothing.

    Asserted structurally: perturbing the catalog module (which every
    executor-less entry declares) or an executor module must move every entry's
    hash between them — so no entry is left with a constant.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.recipes.registry as registry_mod

    def all_hashes(registry):
        return {
            d.recipe_id: d.provenance.implementation_sha256
            for d in registry.descriptors()
        }

    baseline = all_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
    moved = set()

    for module_name in (
        "boomi_mcp.recipes.builtins.catalog",
        "boomi_mcp.recipes.builtins.sync",
        "boomi_mcp.recipes.builtins.fanout",
    ):
        target = importlib.import_module(module_name)
        real_getsource = inspect_module.getsource

        def perturbed(obj, _target=target, _real=real_getsource):
            text = _real(obj)
            if obj is _target:
                return text + "\n# changed\n"
            return text

        original = registry_mod.inspect.getsource
        registry_mod.inspect.getsource = perturbed
        try:
            mutated = all_hashes(build_test_registry(PRODUCTION_REGISTRATIONS))
        finally:
            registry_mod.inspect.getsource = original

        moved |= {rid for rid, h in mutated.items() if h != baseline[rid]}

    assert moved == set(baseline), set(baseline) - moved


#: The three entry points that INVOKE the recipe engine. A module calling any of
#: them is in the execution path by definition.
_ENGINE_ENTRY_POINTS = frozenset(
    {"run_recipes", "run_sync_preset_recipe", "run_fanout_recipe"}
)


def _engine_invoking_modules():
    """Every module that calls a recipe-engine entry point DIRECTLY.

    Parsed, not grepped. An earlier version of this pin searched for the string
    ``recipe_bridge``, which pinned reach through exactly one door: a module
    importing the engine directly was invisible to it, and QA demonstrated the
    blind spot.

    DIRECT calls, one level — not the transitive closure. That is the rule
    ``RECIPE_LAYER_MODULES`` encodes, and calling it a "call graph" scan (as an
    earlier docstring did) overstated it: transitively, the reporting layer
    reaches the engine through ``composition.py``, and the digest deliberately
    excludes it. One level is the boundary between "this module runs recipes" and
    "this module called something that does".

    ``__init__.py`` is INCLUDED here, unlike in the package-contents pin below.
    There a re-export shim genuinely carries no logic; here it could carry a call,
    and skipping it left a hole QA found.
    """
    import ast

    import boomi_mcp

    package_dir = Path(boomi_mcp.__file__).parent
    invoking = set()
    for path in sorted(package_dir.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - defensive
            continue
        called = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    called.add(func.id)
                elif isinstance(func, ast.Attribute):
                    called.add(func.attr)
        if called & _ENGINE_ENTRY_POINTS:
            rel = path.relative_to(package_dir).with_suffix("")
            parts = [p for p in rel.parts if p != "__init__"]
            invoking.add(".".join(["boomi_mcp", *parts]))
    return invoking


def test_every_module_that_invokes_the_engine_is_in_the_layer_digest():
    """The membership RULE, decided by an AST scan rather than by judgement.

    Four consecutive rounds of live QA falsified a BROADER sentence than the list
    backed, each time by finding a module the words covered and the digest did
    not. The problem was never the missing module — it was writing a claim no
    test could check. This pin and the rule in ``registry.py`` are now the same
    statement, so a fifth migrated surface fails here until it is listed.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    invoking = _engine_invoking_modules()
    assert invoking, "the call scan found nothing — the pin would be vacuous"
    missing = invoking - set(RECIPE_LAYER_MODULES)
    assert missing == set(), missing


def test_the_engine_invocation_scan_finds_the_surfaces_we_expect():
    """Guard the guard: an AST scan that matched nothing would pass silently."""
    invoking = _engine_invoking_modules()
    assert {
        "boomi_mcp.patterns.recipe_bridge",
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
    } <= invoking


def test_the_reporting_layer_is_outside_the_digest_by_the_same_rule():
    """The reporting modules never invoke the engine DIRECTLY.

    They do reach it transitively — ``compose_archetypes_action`` calls
    ``compose_archetypes``, which calls ``run_fanout_recipe`` — so the claim is
    about direct invocation, which is the line the digest draws. An earlier
    version of this docstring said they "never invoke the engine" full stop; QA
    walked the live stack and falsified it.

    Stated as a decision, not an oversight: an edit to a response builder can
    change published bytes without changing any recipe's output, and folding it
    in would move every recipe's revision on any response-shape change.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    invoking = _engine_invoking_modules()
    for reporting in (
        "boomi_mcp.categories.integration_authoring",
        "boomi_mcp.categories.integration_import",
        "boomi_mcp.categories.meta_tools",
    ):
        assert reporting not in invoking, reporting
        assert reporting not in RECIPE_LAYER_MODULES, reporting


def test_the_migrated_surfaces_are_in_the_layer_digest():
    """Named explicitly too, so the derived pin above cannot go vacuous.

    If ``recipe_bridge`` were ever renamed, the caller scan would find nothing
    and silently pass. This states the three modules the digest must carry today.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    assert {
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
    } <= set(RECIPE_LAYER_MODULES)


@pytest.mark.parametrize(
    "module_name",
    [
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
    ],
)
def test_source_revision_moves_for_a_migrated_surface(module_name):
    """A migrated archetype can silently change the emitted result.

    QA proved it: renaming an emitted component leaves the build succeeding and
    every published field byte-identical. ``source_revision`` is the only thing
    positioned to notice, so it must.
    """
    import importlib
    import inspect as inspect_module

    import boomi_mcp.build_info as build_info_mod

    baseline = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value

    target = importlib.import_module(module_name)
    real_getsource = inspect_module.getsource

    def perturbed(obj):
        text = real_getsource(obj)
        if obj is target:
            return text + "\n# changed\n"
        return text

    original = build_info_mod.inspect.getsource
    build_info_mod.inspect.getsource = perturbed
    try:
        mutated = build_test_registry(PRODUCTION_REGISTRATIONS).source_revision_value
    finally:
        build_info_mod.inspect.getsource = original

    assert mutated != baseline, module_name


def test_the_downstream_compiler_modules_are_deliberately_outside_the_digest():
    """The stated BOUND, pinned so it stays a decision rather than a drift.

    A recipe run executes the ProcessIR compiler, the component builders and the
    graph verifier. Editing one of those can change the emitted XML with nothing
    published moving — round 4 of live QA measured it. They are excluded because
    including them would make ``source_revision`` a hash of the package, which is
    the same objection that keeps the per-entry digest scoped to one module.

    This test exists so that boundary is asserted, not merely described: if
    someone later widens the list to "fix" it, they have to change this test and
    confront the trade-off rather than sliding into a package hash.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    listed = set(RECIPE_LAYER_MODULES)
    for downstream in (
        "boomi_mcp.compiler.process_ir.pipeline",
        "boomi_mcp.compiler.process_ir.emitter_registry",
        "boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline",
        "boomi_mcp.compiler.process_ir.legacy_adapters.flow_sequence",
        "boomi_mcp.models.process_ir",
        "boomi_mcp.categories.components.process_graph_verifier",
        "boomi_mcp.categories.integration_builder",
    ):
        assert downstream not in listed, downstream


def test_no_listed_module_lies_outside_the_layer_this_issue_owns():
    """Every listed module is one #145 introduced or migrated.

    The converse of the bound above: the digest must not quietly grow into
    someone else's authority either.
    """
    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    # Named EXACTLY outside the recipes package. A ``patterns.archetypes.``
    # prefix would have admitted the four UNMIGRATED archetypes, which the
    # docstring above says are not in the layer — QA caught that the allowance
    # contradicted its own claim.
    allowed_outside = {
        "boomi_mcp.build_info",
        "boomi_mcp.models.recipe_contributions",
        "boomi_mcp.patterns.archetypes.api_to_api_sync",
        "boomi_mcp.patterns.archetypes.api_to_database_sync",
        "boomi_mcp.patterns.composition",
        "boomi_mcp.patterns.recipe_bridge",
        # #146 (M12.11): the typed recipe authoring intent invokes the engine
        # directly, so the engine-invoker scan REQUIRES it in the digest. Both
        # pins have to name it, and that pairing is the point — a new surface
        # joins the layer only by a deliberate edit in two places.
        "boomi_mcp.authoring.workflow",
    }
    for module in RECIPE_LAYER_MODULES:
        assert (
            module.startswith("boomi_mcp.recipes.") or module in allowed_outside
        ), module


def test_an_unregistered_recipe_prerequisite_fails_at_construction():
    """A prerequisite naming something unregistered can never be satisfied.

    The same argument that rejects an unregistered adapter target: a
    registration mistake is not a caller's to diagnose, so it is a build defect
    rather than a ``RECIPE_*`` code at run time (issue #145, live QA).
    """
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="unregistered prerequisite"):
        build_test_registry(
            (
                _reg(
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe",
                            recipe_id="never.registered",
                            recipe_version="1.0.0",
                        ),
                    )
                ),
            )
        )


def test_a_prerequisite_at_the_wrong_version_fails_at_construction():
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="unregistered prerequisite"):
        build_test_registry(
            (
                _reg(recipe_id="test.base", recipe_version="1.0.0"),
                _reg(
                    recipe_id="test.recipe",
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe",
                            recipe_id="test.base",
                            recipe_version="2.0.0",
                        ),
                    ),
                ),
            )
        )


@pytest.mark.parametrize(
    "edges,label",
    [
        ({"cyc.a": "cyc.b", "cyc.b": "cyc.a"}, "mutual 2-cycle"),
        ({"cyc.a": "cyc.b", "cyc.b": "cyc.c", "cyc.c": "cyc.a"}, "3-cycle"),
    ],
    ids=["two", "three"],
)
def test_a_multi_recipe_prerequisite_cycle_fails_at_construction(edges, label):
    """The rule the code already stated, now implemented for every N.

    ``composer.order_invocations`` raises a BARE ``ValueError`` outside the
    ``RecipeError`` envelope when it cannot make progress. An earlier check
    handled a self-loop only, and live QA measured a mutual 2-cycle, a 3-cycle
    and a same-id cross-version cycle all registering cleanly and then escaping
    at run time (issue #145).
    """
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    registrations = tuple(
        _reg(
            recipe_id=source,
            prerequisites=(
                RecipeDependencyV1(
                    kind="recipe", recipe_id=target, recipe_version="1.0.0"
                ),
            ),
        )
        for source, target in edges.items()
    )
    with pytest.raises(ValueError, match="prerequisite cycle"):
        build_test_registry(registrations)


def test_a_same_id_cross_version_cycle_fails_at_construction():
    """Two VERSIONS of one recipe depending on each other is still a cycle."""
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="prerequisite cycle"):
        build_test_registry(
            (
                _reg(
                    recipe_id="cyc.v",
                    recipe_version="1.0.0",
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe", recipe_id="cyc.v", recipe_version="2.0.0"
                        ),
                    ),
                ),
                _reg(
                    recipe_id="cyc.v",
                    recipe_version="2.0.0",
                    is_default=False,
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe", recipe_id="cyc.v", recipe_version="1.0.0"
                        ),
                    ),
                ),
            )
        )


def _depends_on(recipe_id, *targets, **kwargs):
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    return _reg(
        recipe_id=recipe_id,
        prerequisites=tuple(
            RecipeDependencyV1(kind="recipe", recipe_id=t, recipe_version="1.0.0")
            for t in targets
        ),
        **kwargs,
    )


def test_an_acyclic_prerequisite_chain_still_registers():
    """The non-vacuous control: a legitimate chain must not be rejected."""
    registry = build_test_registry(
        (
            _depends_on("chain.a", "chain.b"),
            _depends_on("chain.b", "chain.c"),
            _reg(recipe_id="chain.c"),
        )
    )
    assert len(registry.descriptors()) == 3


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4, 5])
def test_a_diamond_is_not_a_cycle_in_any_registration_order(seed):
    """The control that pins the TRI-COLOUR marking, not just "no cycle".

    A linear chain never re-visits a node, so it cannot tell tri-colour from a
    plain ``visited`` set. A DIAMOND does: ``d`` is reached twice, and without
    marking it BLACK on exit the second visit sees it as GREY and reports a cycle
    that is not there — rejecting every legitimate diamond, and escaping as a
    bare ``ValueError`` from ``stack.index`` at that. Live QA measured exactly
    that mutant surviving the whole suite (issue #145).
    """
    registrations = [
        _depends_on("dia.a", "dia.b", "dia.c"),
        _depends_on("dia.b", "dia.d"),
        _depends_on("dia.c", "dia.d"),
        _reg(recipe_id="dia.d"),
    ]
    rng = random.Random(seed)
    rng.shuffle(registrations)
    registry = build_test_registry(tuple(registrations))
    assert len(registry.descriptors()) == 4


def test_two_disjoint_cycles_are_both_rejected_deterministically():
    """Order must not decide WHICH cycle a caller is shown."""
    registrations = [
        _depends_on("cyc.a", "cyc.b"),
        _depends_on("cyc.b", "cyc.a"),
        _depends_on("cyc.y", "cyc.z"),
        _depends_on("cyc.z", "cyc.y"),
    ]
    messages = set()
    for seed in range(6):
        shuffled = list(registrations)
        random.Random(seed).shuffle(shuffled)
        with pytest.raises(ValueError) as exc:
            build_test_registry(tuple(shuffled))
        messages.add(str(exc.value))
    assert len(messages) == 1, messages


def test_the_cycle_message_names_the_actual_path():
    """The stated rule — "sorted so the cycle shown does not depend on dict
    order" — had no check: every test matched only the substring.

    This test pins ONE half, the deterministic entry point: ``list(graph)`` in
    place of ``sorted(graph)`` produces four different messages across these six
    shuffles. It does NOT pin the path slice — every node here is on the cycle,
    so ``stack.index`` returns 0 and removing the slice leaves this string
    unchanged. ``test_a_cycle_reached_from_an_acyclic_root_reports_only_the_cycle``
    is the one that pins the slice, because its approach node sorts first and is
    therefore ON the stack when the back-edge is found.

    An earlier version of this docstring claimed both halves, and claimed that
    removing the slice "collapses to just the closing edge". Both were false —
    removing it EXPANDS the path — and live QA measured it (issue #145).
    """
    registrations = [
        _depends_on("cyc.a", "cyc.b"),
        _depends_on("cyc.b", "cyc.c"),
        _depends_on("cyc.c", "cyc.a"),
    ]
    messages = set()
    for seed in range(6):
        shuffled = list(registrations)
        random.Random(seed).shuffle(shuffled)
        with pytest.raises(ValueError) as exc:
            build_test_registry(tuple(shuffled))
        messages.add(str(exc.value))
    assert messages == {
        "recipe prerequisite cycle: cyc.a@1.0.0 -> cyc.b@1.0.0 -> cyc.c@1.0.0 -> cyc.a@1.0.0"
    }, messages


def test_a_cycle_reached_from_an_acyclic_root_reports_only_the_cycle():
    """The approach node is not part of the cycle and must not be named.

    The NAMES are load-bearing. With an approach node that sorts after the cycle
    (``app.root`` vs ``app.a``), ``sorted(graph)`` enters the DFS at a cycle
    member, the approach node never lands on the stack, and ``stack.index``
    returns 0 — making the slice a no-op and this assertion unfalsifiable. Live
    QA proved that by asserting ``_i == 0`` inside the product and finding the
    whole suite still green (issue #145).

    ``app.aaa_root`` sorts FIRST, so the DFS reaches the cycle through it, the
    stack is three deep when the back-edge is found, and the slice has real work
    to do.
    """
    with pytest.raises(ValueError) as exc:
        build_test_registry(
            (
                _depends_on("app.aaa_root", "app.m"),
                _depends_on("app.m", "app.n"),
                _depends_on("app.n", "app.m"),
            )
        )
    assert "aaa_root" not in str(exc.value), str(exc.value)
    assert str(exc.value) == (
        "recipe prerequisite cycle: app.m@1.0.0 -> app.n@1.0.0 -> app.m@1.0.0"
    )


def test_the_engine_invocation_scan_does_not_skip_package_inits():
    """``__init__.py`` could carry a call; skipping it left a hole QA found.

    Asserted by construction — the scan's own file walk must not filter them —
    rather than by hoping no ``__init__`` ever gains one.
    """
    import inspect as inspect_module

    source = inspect_module.getsource(_engine_invoking_modules)
    assert 'if path.name == "__init__.py"' not in source
    assert '__init__' in source  # the deliberate name-stripping, not a skip


def test_an_unknown_recipe_registry_capability_subject_fails_at_construction():
    """The seventh authority, which used to slip through.

    ``recipe_registry`` subjects cannot be checked per-registration — a recipe may
    require one registered later in the tuple — so they are swept at the END of
    construction. Before that sweep existed, a typo'd subject was accepted and
    then reported as ``RECIPE_CAPABILITY_GATED`` at preflight: exactly the
    "blame the caller's platform for our typo" misdiagnosis construction-time
    enforcement is supposed to prevent (issue #145, live QA).
    """
    with pytest.raises(ValueError, match="unknown recipe_registry subject"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority="recipe_registry",
                            subject="no.such.recipe",
                            required_state="supported",
                        ),
                    )
                ),
            )
        )


def test_a_recipe_registry_subject_registered_later_still_resolves():
    """The reason the sweep is deferred rather than per-registration."""
    registry = build_test_registry(
        (
            _reg(
                recipe_id="a.first",
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="recipe_registry",
                        subject="z.later",
                        required_state="supported",
                    ),
                ),
            ),
            _reg(recipe_id="z.later"),
        )
    )
    registry.preflight_capabilities(registry.resolve("a.first"))


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): "at least this" was inverted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "authority,subject",
    [
        ("component_builder", "process"),
        ("process_emitter", "branch"),
        ("process_body", "branch_leg.step"),
    ],
)
@pytest.mark.parametrize("state", ["supported", "emittable", "plannable-only"])
def test_a_registered_membership_subject_satisfies_every_positive_state(
    authority, subject, state
):
    """A membership authority answers "is it there", so presence is the ceiling.

    The check used to be ``present and "supported" in accepted``, which inverted
    the "at least this" rule on five of the seven authorities: a
    ``plannable-only`` requirement — the WEAKEST ask — could never be satisfied,
    because ``_STATE_SATISFIES["plannable-only"]`` deliberately contains no
    ``supported``. Latent while every built-in asks for ``supported``, which is
    why a live-QA mutant read inert before this was found.
    """
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority=authority, subject=subject, required_state=state
                    ),
                )
            ),
        )
    )
    registry.preflight_capabilities(registry.resolve("test.recipe"))


@pytest.mark.parametrize(
    "authority,absent_subject",
    [
        ("component_builder", "not.a.component.type"),
        ("process_emitter", "not.an.emitter"),
        ("process_body", "not.a.body.slot"),
        ("recipe_registry", "not.a.registered.recipe"),
    ],
)
def test_an_absent_membership_subject_is_rejected_at_construction(
    authority, absent_subject
):
    """The converse guard, made non-vacuous.

    Its first version asked for ``recipe_registry/test.recipe`` — the id the test
    registry had just REGISTERED — so a mutant that ignored presence entirely
    passed the whole suite. QA caught that (issue #145). These subjects are
    genuinely absent, and an absent subject is a build defect: it fails at
    construction, before any preflight.
    """
    with pytest.raises(ValueError, match="unknown"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority=authority,
                            subject=absent_subject,
                            required_state="supported",
                        ),
                    )
                ),
            )
        )


def test_a_gated_stateful_subject_still_fails_preflight():
    """Widening the membership branch must not have touched the stateful ones."""
    for subject in ("joins", "loops", "keyed_cache"):
        registry = build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority="process_ir",
                            subject=subject,
                            required_state="supported",
                        ),
                    )
                ),
            )
        )
        with pytest.raises(RecipeError):
            registry.preflight_capabilities(registry.resolve("test.recipe"))


@pytest.mark.parametrize(
    "authority,subject,state",
    [
        ("process_ir", "rich_branch_decision_bodies", "emittable"),
        ("process_ir", "rich_branch_decision_bodies", "plannable-only"),
        ("system_topology", "process", "supported"),
    ],
)
def test_an_unsatisfiable_authority_state_pair_fails_at_construction(
    authority, subject, state
):
    """A required_state the authority can NEVER report is a build defect.

    ``process_ir`` only ever reports supported/gated/unsupported, so requiring
    ``emittable`` of it is impossible by construction. Surfacing that at preflight
    as ``RECIPE_CAPABILITY_GATED`` would blame the caller's platform for our own
    impossible declaration (issue #145, live QA).
    """
    with pytest.raises(ValueError, match="can never report it"):
        build_test_registry(
            (
                _reg(
                    capability_requirements=(
                        RecipeCapabilityRequirementV1(
                            authority=authority, subject=subject, required_state=state
                        ),
                    )
                ),
            )
        )


def test_the_runtime_capability_path_gets_the_same_guards_as_construction():
    """A CONTRIBUTED ``RequireCapability`` is checked like a descriptor one.

    The runtime path used to get neither the unknown-subject nor the
    unsatisfiable-state guard, so a typo'd subject reported "not satisfied"
    rather than "you asked for something that does not exist" — and round 8's
    mutant read inert because of it (issue #145, live QA). Both return ``False``
    here rather than raising: a contribution is caller-adjacent input, so an
    unanswerable requirement is an unmet one.
    """
    registry = build_test_registry((_reg(),))

    # A typo'd subject on a membership authority.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_emitter",
            subject="no.such.emitter",
            required_state="supported",
        )
    )
    # A typo'd subject on a stateful authority.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_ir", subject="no.such.feature", required_state="supported"
        )
    )
    # An unregistered recipe id.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="recipe_registry",
            subject="not.registered",
            required_state="supported",
        )
    )
    # An (authority, state) pair the authority can never report.
    assert not registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_ir",
            subject="rich_branch_decision_bodies",
            required_state="emittable",
        )
    )
    # ...and the legitimate case still passes, so this is not a blanket False.
    assert registry.capability_satisfied(
        RecipeCapabilityRequirementV1(
            authority="process_ir",
            subject="rich_branch_decision_bodies",
            required_state="supported",
        )
    )


def test_a_capability_diagnostic_does_not_publish_the_dark_subject():
    """``public_payload`` redacts capability subjects; the diagnostic must too.

    Publishing them here would have leaked through the failure path exactly what
    the descriptor projection withholds — the emitter registry's own keys.
    """
    registry = build_test_registry(
        (
            _reg(
                capability_requirements=(
                    RecipeCapabilityRequirementV1(
                        authority="process_ir", subject="joins", required_state="supported"
                    ),
                )
            ),
        )
    )
    with pytest.raises(RecipeError) as exc:
        registry.preflight_capabilities(registry.resolve("test.recipe"))
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.target == "capability:process_ir"
    assert "joins" not in diagnostic.target


def test_an_async_generator_executor_is_rejected():
    """``iscoroutinefunction`` is False for ``async def f(): yield``.

    ``isfunction`` is True, so it slipped through construction and surfaced at
    run time as ``RECIPE_CONTRIBUTION_INVALID`` — a build defect wearing a
    caller-facing code (issue #145, live QA).
    """

    async def async_gen(inp):  # pragma: no cover - rejected at registration
        yield ()

    with pytest.raises(ValueError, match="coroutine"):
        build_test_registry((_reg(executor=async_gen),))


def test_a_plain_generator_executor_is_rejected():
    def gen(inp):  # pragma: no cover - rejected at registration
        yield ()

    with pytest.raises(ValueError, match="generator"):
        build_test_registry((_reg(executor=gen),))


def test_a_self_dependent_prerequisite_is_rejected_at_construction():
    """It used to build cleanly and then raise a BARE ValueError mid-run.

    The composer's cycle guard fired from inside ``run_recipes``, outside the
    ``RecipeError`` envelope, so only the MCP layer's last-line ``except
    Exception`` caught it (issue #145, live QA). It is a build defect.
    """
    from boomi_mcp.recipes.contracts import RecipeDependencyV1

    with pytest.raises(ValueError, match="prerequisite cycle"):
        build_test_registry(
            (
                _reg(
                    prerequisites=(
                        RecipeDependencyV1(
                            kind="recipe",
                            recipe_id="test.recipe",
                            recipe_version="1.0.0",
                        ),
                    )
                ),
            )
        )


def test_running_a_registered_but_non_executable_entry_says_so():
    """An advisory/adapter id IS registered — "not found" sent callers hunting
    for a typo in a name that was correct (issue #145, live QA)."""
    from boomi_mcp.errors import RECIPE_REQUEST_INVALID

    registry = production_registry()
    for recipe_id in (
        RECIPE_ADVISORY_INTEGRATION_DESIGN,
        "boomi.adapter.compose_archetypes",
    ):
        descriptor = registry.resolve(recipe_id)
        with pytest.raises(RecipeError) as exc:
            registry.executor_for(descriptor)
        diagnostic = exc.value.diagnostics[0]
        assert diagnostic.code == RECIPE_REQUEST_INVALID
        assert diagnostic.target.startswith("not_executable:")


def test_registry_revision_is_stable_across_semver_build_metadata():
    """Build metadata is ignored FOR PRECEDENCE, so two versions differing only
    in it compare equal — and their order fell to dict insertion order."""
    def regs(order):
        return tuple(
            _reg(recipe_id="r.build", recipe_version=v, is_default=(i == 0))
            for i, v in enumerate(order)
        )

    a = build_test_registry(regs(["1.0.0+alpha", "1.0.0+beta"]))
    b = build_test_registry(regs(["1.0.0+beta", "1.0.0+alpha"]))
    assert [d.recipe_version for d in a.descriptors()] == [
        d.recipe_version for d in b.descriptors()
    ]


# ---------------------------------------------------------------------------
# Codex-review regressions (issue #145)
# ---------------------------------------------------------------------------


def test_an_extra_live_version_is_reported_as_skew():
    """``live_only`` compares (id, VERSION), not id.

    An id-only subtraction is blind to an extra VERSION of a known recipe: a
    live registry carrying x@1.0.0 AND x@2.0.0 reported ``match`` against a
    fully-hashed expectation naming only x@1.0.0 — the parallel-version support
    this registry advertises (issue #145, Codex review).
    """
    def reg(version, default):
        return _reg(recipe_id="x.multi", recipe_version=version, is_default=default)

    registry = build_test_registry((reg("1.0.0", True), reg("2.0.0", False)))
    first = next(d for d in registry.descriptors() if d.recipe_version == "1.0.0")
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(
                    recipe_id="x.multi",
                    recipe_version="1.0.0",
                    implementation_sha256=first.provenance.implementation_sha256,
                ),
            )
        )
    )
    assert skew.status == "mismatch"
    assert skew.live_only == ("x.multi@2.0.0",)


def test_an_expectation_naming_every_live_version_still_matches():
    """The converse: the fix must not make a complete expectation mismatch."""
    def reg(version, default):
        return _reg(recipe_id="x.multi", recipe_version=version, is_default=default)

    registry = build_test_registry((reg("1.0.0", True), reg("2.0.0", False)))
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=tuple(
                ExpectedRecipeEntryV1(
                    recipe_id=d.recipe_id,
                    recipe_version=d.recipe_version,
                    implementation_sha256=d.provenance.implementation_sha256,
                )
                for d in registry.descriptors()
            )
        )
    )
    assert skew.status == "match"
    assert skew.live_only == ()


def test_a_wholly_unknown_live_id_is_reported_by_id_not_by_version():
    """An id the caller never claimed to know about is reported once, not per
    version — otherwise one unknown recipe with five versions drowns the finding."""
    registry = build_test_registry((_reg(recipe_id="a.known"), _reg(recipe_id="z.unknown")))
    known = next(d for d in registry.descriptors() if d.recipe_id == "a.known")
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(
                    recipe_id="a.known",
                    recipe_version=known.recipe_version,
                    implementation_sha256=known.provenance.implementation_sha256,
                ),
            )
        )
    )
    assert skew.live_only == ("z.unknown",)


def test_the_layer_module_list_follows_the_ACTIVE_import_namespace():
    """``PACKAGE_NAME`` is the DISTRIBUTION name; the import prefix is not.

    Run in a SUBPROCESS that loads the registry as ``src.boomi_mcp.*``, because
    under pytest the active namespace already IS ``boomi_mcp`` — so asserting
    the derivation in-process passes just as well with the prefix hard-coded,
    which is how the first version of this test managed to guard nothing
    (issue #145, live QA).
    """
    import subprocess
    import sys

    # Assert the NEGATIVE first: ``boomi_mcp`` must be unreachable in the child.
    # ``_clean_env`` shuts PYTHONPATH, but the editable-install ``.pth`` is a
    # second channel — it only fails to leak today because it points at a stale
    # path. Depending on which channel happens to be shut is not isolation
    # (issue #145, live QA).
    probe = (
        "import sys; sys.path.insert(0, '.')\n"
        "try:\n"
        "    import boomi_mcp\n"
        "except ImportError:\n"
        "    pass\n"
        "else:\n"
        "    raise SystemExit('LEAKED: boomi_mcp is importable in the probe')\n"
        "from src.boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES\n"
        "print(RECIPE_LAYER_MODULES[0])"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(_project_root),
        capture_output=True,
        text=True,
        env=_clean_env(),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().startswith("src.boomi_mcp."), result.stdout

    # ...and the DISTRIBUTION name in published provenance is unaffected.
    assert production_registry().descriptors()[0].provenance.package_name == "boomi_mcp"


def test_the_recipe_layer_itself_loads_under_the_src_namespace():
    """The property the prefix exists to guarantee, for the modules #145 owns.

    HONEST BOUND: the three MIGRATED SURFACES (``patterns/composition`` and the
    two archetypes) do NOT load under ``src.boomi_mcp`` — five pre-existing
    files, ``connector_builder.py`` among them, use absolute ``boomi_mcp.``
    imports, and that has been true since long before this issue (verified
    against the baseline tree). Fixing those is not #145's to do, so this pins
    what the prefix derivation actually buys: every module the recipe layer owns
    now resolves under whichever namespace loaded it.
    """
    import subprocess
    import sys

    owned = [
        "build_info",
        "models.recipe_contributions",
        "patterns.recipe_bridge",
        "recipes.builtins.catalog",
        "recipes.builtins.fanout",
        "recipes.builtins.sync",
        "recipes.composer",
        "recipes.contracts",
        "recipes.engine",
        "recipes.errors",
        "recipes.materialization",
        "recipes.registry",
    ]
    probe = (
        "import sys, importlib; sys.path.insert(0, '.')\n"
        "try:\n"
        "    import boomi_mcp\n"
        "except ImportError:\n"
        "    pass\n"
        "else:\n"
        "    raise SystemExit('LEAKED: boomi_mcp is importable in the probe')\n"
        f"for m in {owned!r}: importlib.import_module('src.boomi_mcp.' + m)\n"
        "print('ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(_project_root),
        capture_output=True,
        text=True,
        env=_clean_env(),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_every_layer_module_is_importable_under_the_active_namespace():
    """Under the namespace pytest actually runs in, ALL of them load."""
    import importlib

    from boomi_mcp.recipes.registry import RECIPE_LAYER_MODULES

    for module in RECIPE_LAYER_MODULES:
        importlib.import_module(module)


def test_an_adapter_naming_an_unregistered_target_fails_at_construction():
    """An adapter's declared target must name a REGISTERED (id, version).

    Nothing required it before, so an adapter could point at a version that did
    not exist and every surface reading ``adapter_target`` would report a recipe
    nobody could run (issue #145, live QA).
    """
    with pytest.raises(ValueError, match="unregistered adapter target"):
        build_test_registry(
            (
                RecipeRegistrationV1(
                    recipe_id="test.adapter",
                    recipe_version="1.0.0",
                    entry_kind="compatibility_adapter",
                    is_default=True,
                    adapter_target=RecipeReferenceV1(
                        recipe_id="never.registered", recipe_version="1.0.0"
                    ),
                ),
            )
        )


def test_an_adapter_pointing_at_a_non_executable_entry_fails_at_construction():
    """Adapting TO an advisory pointer would produce an unrunnable route."""
    with pytest.raises(ValueError, match="not executable"):
        build_test_registry(
            (
                RecipeRegistrationV1(
                    recipe_id="test.adapter",
                    recipe_version="1.0.0",
                    entry_kind="compatibility_adapter",
                    is_default=True,
                    adapter_target=RecipeReferenceV1(
                        recipe_id="test.advisory", recipe_version="1.0.0"
                    ),
                ),
                RecipeRegistrationV1(
                    recipe_id="test.advisory",
                    recipe_version="1.0.0",
                    entry_kind="advisory",
                    is_default=True,
                ),
            )
        )


def test_every_production_adapter_target_is_registered_and_executable():
    """The production catalog satisfies the rule it now enforces."""
    registry = production_registry()
    by_key = {(d.recipe_id, d.recipe_version): d for d in registry.descriptors()}
    adapters = [d for d in registry.descriptors() if d.adapter_target is not None]
    assert len(adapters) == 3
    for adapter in adapters:
        key = (adapter.adapter_target.recipe_id, adapter.adapter_target.recipe_version)
        assert key in by_key, key
        assert by_key[key].entry_kind == "executable_recipe", key


def test_live_only_is_globally_sorted_in_the_mixed_case():
    """Two sorted runs concatenated is not a sorted collection."""
    registry = build_test_registry(
        (
            _reg(recipe_id="m.known", recipe_version="1.0.0"),
            _reg(recipe_id="m.known", recipe_version="2.0.0", is_default=False),
            _reg(recipe_id="a.unknown"),
            _reg(recipe_id="z.unknown"),
        )
    )
    known = next(
        d for d in registry.descriptors()
        if d.recipe_id == "m.known" and d.recipe_version == "1.0.0"
    )
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(
                    recipe_id="m.known",
                    recipe_version="1.0.0",
                    implementation_sha256=known.provenance.implementation_sha256,
                ),
            )
        )
    )
    assert skew.status == "mismatch"
    assert list(skew.live_only) == sorted(skew.live_only), skew.live_only
    assert set(skew.live_only) == {"a.unknown", "m.known@2.0.0", "z.unknown"}


def test_a_version_mismatch_is_not_also_reported_as_live_only():
    """One fact, one finding. Reporting it twice makes the collections disagree
    about how many problems there are."""
    registry = build_test_registry((_reg(recipe_id="v.one", recipe_version="1.0.0"),))
    skew = registry.compare(
        ExpectedRecipeRegistryV1(
            entries=(
                ExpectedRecipeEntryV1(recipe_id="v.one", recipe_version="9.9.9"),
            )
        )
    )
    assert skew.status == "mismatch"
    assert [m.recipe_id for m in skew.version_mismatches] == ["v.one"]
    assert skew.live_only == ()


# ---------------------------------------------------------------------------
# Live-QA regression (issue #145): build defects with no test at all
# ---------------------------------------------------------------------------


def test_an_executable_recipe_without_an_input_model_fails_at_construction():
    with pytest.raises(ValueError, match="requires both an executor and an input model"):
        build_test_registry((_reg(input_model=None),))


def test_an_executable_recipe_without_an_executor_fails_at_construction():
    with pytest.raises(ValueError, match="requires both an executor and an input model"):
        build_test_registry((_reg(executor=None),))


def test_an_executable_recipe_naming_an_adapter_target_fails_at_construction():
    """Only a compatibility adapter adapts TO something."""
    with pytest.raises(ValueError, match="must not name an adapter target"):
        build_test_registry(
            (
                _reg(
                    adapter_target=RecipeReferenceV1(
                        recipe_id="x.y", recipe_version="1.0.0"
                    )
                ),
            )
        )


def test_an_executable_recipe_without_a_conflict_policy_fails_at_construction():
    """A recipe that can write a contested slot must say what it will merge."""
    with pytest.raises(ValueError, match="must declare a conflict policy"):
        build_test_registry((_reg(conflict_policy=None),))


def test_a_nested_executor_is_rejected_at_construction():
    """``<locals>`` in the qualname — a function defined inside another.

    Its source is readable but its enclosing scope is not, so the same
    provenance argument that rejects closures applies.
    """

    def outer():
        def inner(inp):  # pragma: no cover - rejected at registration
            return ()

        return inner

    with pytest.raises(ValueError, match="module level"):
        build_test_registry((_reg(executor=outer()),))


def _raise_value_error_sites(module):
    """Every ``raise ValueError`` line in a module, and whether it is pragma'd.

    Matches BOTH spellings: ``raise ValueError(...)`` (an ``ast.Call``) and a bare
    ``raise ValueError`` (an ``ast.Name``). Counting only the call form left the
    bare one silent — and the bare form is precisely what bypasses a guard that
    says it counts them all (issue #145, live QA).
    """
    import ast

    source = Path(module.__file__).read_text()
    lines = source.splitlines()
    sites = {}
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        exc = node.exc
        name = (
            getattr(exc.func, "id", None)
            if isinstance(exc, ast.Call)
            else getattr(exc, "id", None)
        )
        if name != "ValueError":
            continue
        pragma = any(
            "pragma: no cover" in lines[i]
            for i in range(max(0, node.lineno - 3), node.lineno)
        )
        sites[node.lineno] = pragma
    return sites


def test_every_reachable_registry_build_defect_is_exercised_by_a_test():
    """A real REACHABILITY assertion, not a count.

    Runs this module's own tests in a subprocess under ``sys.settrace`` and
    requires every non-pragma ``raise ValueError`` in ``registry.py`` to have been
    executed. An earlier version was a pinned count, justified by "asserting
    reachability needs a coverage tool this repo does not carry" — which live QA
    refuted: the stdlib tracer below is that assertion, and it is green
    (issue #145).

    The tracer returns ``None`` for frames outside the target file, so only
    ``registry.py`` is line-traced and the run stays fast.
    """
    import subprocess
    import sys

    if os.environ.get("BOOMI_RECIPE_TRACE_CHILD"):
        pytest.skip("re-entry guard: this test spawns the run it measures")

    from boomi_mcp.recipes import registry as registry_module

    sites = _raise_value_error_sites(registry_module)
    expected = sorted(line for line, pragma in sites.items() if not pragma)
    assert expected, "no raise sites found — the matcher is broken"

    driver = f"""
import sys, pathlib, json
target = str(pathlib.Path({str(Path(registry_module.__file__))!r}).resolve())
hit = set()
def tracer(frame, event, arg):
    if frame.f_code.co_filename != target:
        return None
    if event == "line":
        hit.add(frame.f_lineno)
    return tracer
import pytest
sys.settrace(tracer)
code = pytest.main(["-q", "-p", "no:cacheprovider", {str(Path(__file__))!r}])
sys.settrace(None)
print("TRACE:" + json.dumps(sorted(hit)))
raise SystemExit(0 if code == 0 else 1)
"""
    result = subprocess.run(
        [sys.executable, "-c", driver],
        cwd=str(_project_root),
        capture_output=True,
        text=True,
        env={
            **_inherited_env(),
            "PYTHONPATH": str(_project_root / "src"),
            # Without this the child re-runs THIS test, which spawns another
            # child, forever.
            "BOOMI_RECIPE_TRACE_CHILD": "1",
        },
    )
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
    marker = [ln for ln in result.stdout.splitlines() if ln.startswith("TRACE:")]
    assert marker, result.stdout[-2000:]
    hit = set(json.loads(marker[-1][len("TRACE:"):]))

    missed = [line for line in expected if line not in hit]
    assert missed == [], (
        f"registry.py build defects with no test: lines {missed}. "
        "Either add a test or mark the site '# pragma: no cover' with a reason."
    )

    # The ESCAPE HATCH is pinned too. Excluding pragma'd sites and asserting
    # nothing about them turned "edit a pinned tuple" into "type a comment": a
    # new untested raise fails above, and adding a pragma to the same line made
    # it pass silently (issue #145, live QA).
    # The ESCAPE HATCH is pinned by IDENTITY, not cardinality. A count is
    # substitution-blind: removing one pragma'd raise and adding a different
    # untested one in the same commit keeps it at 3 and the suite green (issue
    # #145, live QA). The reason text is what makes two exemptions distinct.
    exempted = _pragma_raise_identities(registry_module)
    assert exempted == (
        (
            "RecipeRegistry._check_entry_kind",
            "the Literal already bounds this",
            'raise ValueError(f"unknown recipe entry kind {kind!r}")',
        ),
        (
            "RecipeRegistry._implementation_identity",
            "environment",
            'raise ValueError( "recipe provenance requires readable source for "'
            ' f"{catalog_module}" ) from exc',
        ),
        (
            "RecipeRegistry._implementation_identity",
            "environment",
            'raise ValueError( f"recipe provenance requires readable source for "'
            ' f"{module}.{symbol}" ) from exc',
        ),
    ), (
        f"registry.py's exempted raise sites changed to {exempted}. "
        "A pragma is an assertion that the site is unreachable — justify it here."
    )


def _pragma_reason(line):
    """The justification text on a ``# pragma: no cover`` line, or ``""``.

    Stops at a following tool directive: ``# pragma: no cover  # type: ignore``
    has no reason, and consuming the directive as one was how a bare exemption
    passed (issue #145, live QA).
    """
    marker = "pragma: no cover"
    if marker not in line:
        return ""
    tail = line.split(marker, 1)[1]
    tail = tail.split("#", 1)[0]
    return tail.strip(" -\u2014\t").strip()


def _enclosing_qualname(tree, lineno):
    """The dotted name of the function/class enclosing a line."""
    import ast

    best = []
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            continue
        end = getattr(node, "end_lineno", node.lineno)
        if node.lineno <= lineno <= end:
            best.append((node.lineno, node.name))
    return ".".join(name for _, name in sorted(best)) or "<module>"


def _pragma_raise_identities(module):
    """``(enclosing qualname, reason)`` per pragma'd ``raise ValueError``.

    IDENTITY, not a count and not a bare reason multiset. A count is blind to
    substitution; so is a reason multiset when two exemptions legitimately share
    a reason string — which two of these three do ("environment"), so deleting
    one and adding a different untested raise with the same reason kept the tuple
    equal and the suite green (issue #145, live QA). The enclosing function is
    what makes two same-reason exemptions distinguishable.
    """
    import ast

    source = Path(module.__file__).read_text()
    tree = ast.parse(source)
    lines = source.splitlines()
    by_line = {
        node.lineno: node
        for node in ast.walk(tree)
        if isinstance(node, ast.Raise) and node.exc is not None
    }
    identities = []
    for line_number, pragma in _raise_value_error_sites(module).items():
        if not pragma:
            continue
        reason = ""
        for i in range(max(0, line_number - 3), line_number):
            reason = _pragma_reason(lines[i]) or reason
        # The raise's OWN source too. Two exemptions can share a function AND a
        # reason — two of these three do — so neither is a discriminator on its
        # own; the message is what makes them distinct.
        node = by_line.get(line_number)
        message = " ".join((ast.get_source_segment(source, node) or "").split())
        identities.append(
            (_enclosing_qualname(tree, line_number), reason, message)
        )
    return tuple(sorted(identities))


def test_every_pragma_in_the_registry_states_a_reason():
    """``# pragma: no cover`` is a claim; a bare one is a claim with no argument.

    One site carried a bare pragma, so the guard's own instruction ("mark it with
    a reason") was unenforced (issue #145, live QA).
    """
    from boomi_mcp.recipes import registry as registry_module

    bare = []
    for number, line in enumerate(
        Path(registry_module.__file__).read_text().splitlines(), start=1
    ):
        if "pragma: no cover" not in line:
            continue
        reason = _pragma_reason(line)
        # At least one real WORD (3+ letters). ``- .`` is not a justification,
        # and a following ``# type: ignore`` is a different directive, not this
        # one's reason (issue #145, live QA). One word is enough — "environment"
        # says what it needs to. 3 rather than 4 so ``- see ADR-001 §5`` passes:
        # the bar is "not punctuation", not "verbose".
        if not re.findall(r"[A-Za-z]{3,}", reason):
            bare.append(number)
    assert bare == [], f"pragma with no usable reason at lines {bare}"


def test_the_raise_site_matcher_sees_the_bare_spelling():
    """Guard the guard: ``raise ValueError`` with no call must be counted.

    The matcher missing it is what made the previous census silent on exactly the
    form that bypasses it.
    """
    import ast
    import types

    module = types.SimpleNamespace()
    with tempfile.NamedTemporaryFile(
        "w", suffix=".py", delete=False, encoding="utf-8"
    ) as handle:
        handle.write("def f(x):\n    if x:\n        raise ValueError\n")
        module.__file__ = handle.name
    try:
        sites = _raise_value_error_sites(module)
        assert sites == {3: False}, sites
    finally:
        os.unlink(module.__file__)


@pytest.mark.parametrize(
    "line,expected",
    [
        ("x = 1  # pragma: no cover - environment", "environment"),
        ("x = 1  # pragma: no cover - see ADR-001 §5", "see ADR-001 §5"),
        ("x = 1  # pragma: no cover", ""),
        ("x = 1  # pragma: no cover  # type: ignore", ""),
        ("x = 1  # pragma: no cover - .", "."),
        ("x = 1", ""),
    ],
)
def test_the_pragma_reason_parser_stops_at_a_following_directive(line, expected):
    """``# pragma: no cover  # type: ignore`` has NO reason.

    Consuming the following tool directive as the justification is how a bare
    exemption passed the lint (issue #145, live QA). The parser had no test of
    its own — only the lint that uses it — so reverting either half of the fix
    left the suite green.
    """
    assert _pragma_reason(line) == expected


@pytest.mark.parametrize(
    "reason,accepted",
    [
        ("environment", True),
        ("see ADR-001 §5", True),
        ("Literal-bounded", True),
        (".", False),
        ("", False),
        ("x", False),
    ],
)
def test_the_reason_bar_is_not_punctuation_rather_than_verbose(reason, accepted):
    """The bar is deliberately loose: one real word is enough.

    A 4-letter minimum rejected ``see ADR-001 §5``, which is a perfectly good
    justification — so the threshold is 3. ``- .`` is still rejected, which is
    the case that matters.
    """
    assert bool(re.findall(r"[A-Za-z]{3,}", reason)) is accepted


# ---------------------------------------------------------------------------
# The walk must not mistake DATA for a schema node
#
# Fourth appearance of one class. The first three were patched one constructed
# counter-example at a time; this round enumerates the position class from
# pydantic's own declarations and pins the enumeration with a tripwire
# (issue #145, Codex review).
# ---------------------------------------------------------------------------


def test_a_type_legal_custom_error_context_is_not_read_as_a_validator():
    """``custom_error_context`` is declared ``dict[str, str | int | float]``, so
    ``{"type": "function-wrap"}`` is a perfectly legal value — a flat mapping of
    strings. Walking it as a node refused a genuinely closed, frozen model."""
    from typing import Annotated, Literal, Union

    from pydantic import ConfigDict, Discriminator, Tag

    class Cat(RecipeInputBase):
        kind: Literal["cat"] = "cat"

    class Dog(RecipeInputBase):
        kind: Literal["dog"] = "dog"

    def _pick(value: object) -> str:
        return "cat"

    class Holder(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)

        pet: Annotated[
            Union[Annotated[Cat, Tag("cat")], Annotated[Dog, Tag("dog")]],
            Discriminator(
                _pick,
                custom_error_type="bad_pet",
                custom_error_message="bad pet",
                custom_error_context={"type": "function-wrap"},
            ),
        ] = Cat()

    # The poisoned value really is where the walk would reach it...
    found = []
    stack = [Holder.__pydantic_core_schema__]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if node.get("custom_error_context"):
                found.append(node["custom_error_context"])
            stack.extend(v for v in node.values() if isinstance(v, (dict, list)))
        elif isinstance(node, list):
            stack.extend(v for v in node if isinstance(v, (dict, list)))
    assert {"type": "function-wrap"} in found

    # ...and the gates accept the model anyway.
    registry_module._check_input_schema_closed("probe", Holder)
    registry_module._check_input_model_forbids_extras("probe", Holder)

    # Control: the model is genuinely closed.
    with pytest.raises(Exception):
        Holder.model_validate({"pet": {"kind": "cat"}, "smuggled": 1})


@pytest.mark.parametrize("mode", ["wrap", "plain"])
def test_a_wrap_or_plain_field_serializer_is_not_refused(mode):
    """``serialization`` is excluded for the BAN's sake, not only for data safety.

    ``@field_serializer(mode="wrap"|"plain")`` compiles to a SER-schema node whose
    ``type`` is the string ``function-wrap``/``function-plain`` — byte-identical to
    the validator node types the ban keys on. Serializers run on output and cannot
    affect extras rejection, so they are excluded rather than banned. Drop the
    exclusion and every model carrying a wrap serializer is refused.
    """
    from typing import Any as AnyType

    from pydantic import ConfigDict, field_serializer

    if mode == "wrap":

        class Model(RecipeInputBase):
            model_config = ConfigDict(extra="forbid", frozen=True)
            name: str = "x"

            @field_serializer("name", mode="wrap")
            def _ser(self, value: AnyType, nxt: AnyType) -> AnyType:
                return nxt(value)

    else:

        class Model(RecipeInputBase):
            model_config = ConfigDict(extra="forbid", frozen=True)
            name: str = "x"

            @field_serializer("name", mode="plain")
            def _ser(self, value: AnyType) -> AnyType:
                return value

    # The collision is real — the node is there, under ``serialization``.
    found = []
    stack = [(Model.__pydantic_core_schema__, "root")]
    while stack:
        node, path = stack.pop()
        if isinstance(node, dict):
            if node.get("type") in ("function-wrap", "function-plain"):
                found.append(path)
            stack.extend(
                (v, f"{path}/{k}") for k, v in node.items() if isinstance(v, (dict, list))
            )
        elif isinstance(node, list):
            stack.extend((v, path) for v in node if isinstance(v, (dict, list)))
    assert found, "expected a ser-schema node carrying the banned type string"
    assert all(p.endswith("/serialization") for p in found), found

    # And the gate accepts the model regardless.
    registry_module._check_input_model_forbids_extras("probe", Model)


def test_non_schema_keys_covers_every_declared_free_form_position():
    """Tripwire, so the exclusion list fails LOUDLY when pydantic moves.

    Re-derives from ``pydantic_core.core_schema`` every key that its own
    TypedDicts say can hold a container but can never (transitively) hold a
    ``CoreSchema``. Those are exactly the positions where a walk-everything
    traversal can mistake data for a node.

    LIMIT, stated rather than implied: this reads DECLARATIONS, and the compiled
    form is not always the declared one — ``function`` is annotated ``Callable``
    yet compiles to a ``{"type": "with-info", ...}`` dict, which is why it is
    excluded by measurement below rather than by this derivation. A new position
    of that kind would not be caught here.
    """
    import typing

    from pydantic_core import core_schema as cs

    typed_dicts = {
        name: obj
        for name in dir(cs)
        if typing.is_typeddict(obj := getattr(cs, name))
    }
    assert len(typed_dicts) > 40, "core_schema TypedDicts not introspectable"

    def annotations_of(td):
        try:
            return typing.get_type_hints(td, include_extras=False)
        except Exception:  # pragma: no cover - a pydantic move, not a repo defect
            return dict(getattr(td, "__annotations__", {}))

    def bears_schema(ann, depth=0):
        if depth > 6:
            return True  # unknown -> assume schema-bearing, i.e. keep walking
        if ann is cs.CoreSchema:
            return True
        if typing.is_typeddict(ann):
            return any(bears_schema(v, depth + 1) for v in annotations_of(ann).values())
        text = str(ann)
        if "CoreSchema" in text and "CoreSchemaType" not in text:
            return True
        args = typing.get_args(ann)
        return any(bears_schema(a, depth + 1) for a in args) if args else False

    def admits_mapping(ann, depth=0):
        """Can a value here be a MAPPING, or a sequence containing one?

        Only those can be mistaken for a node: the walk pushes ``dict`` values and
        the ``dict`` items of a ``list``, and nothing else. A ``set[int|str]``
        (``include``/``exclude``) or a ``list[str]`` (``allowed_schemes``,
        ``alias``, ``discriminator``) is never walked, so flagging it would be
        noise that trains the next reader to widen the exclusion list for nothing.
        """
        if depth > 6:
            return True
        origin = typing.get_origin(ann)
        # A class object, a callable, or a literal VALUE is never a walked node.
        if origin in (type, typing.Literal) or "Callable" in str(origin):
            return False
        if origin in (dict, typing.Mapping):
            return True
        text = str(ann).replace("typing.", "")
        if text in ("Any", "<class 'object'>"):
            return True
        if origin in (list, tuple, set, frozenset):
            return any(admits_mapping(a, depth + 1) for a in typing.get_args(ann))
        args = typing.get_args(ann)
        return any(admits_mapping(a, depth + 1) for a in args) if args else False

    ever, never = set(), set()
    for td in typed_dicts.values():
        for key, ann in annotations_of(td).items():
            (ever if bears_schema(ann) else never).add(key)
    never -= ever

    walkable = {
        key
        for td in typed_dicts.values()
        for key, ann in annotations_of(td).items()
        if key in never and admits_mapping(ann)
    }
    assert walkable, "derivation produced nothing — it has stopped measuring"

    uncovered = walkable - registry_module._NON_SCHEMA_KEYS
    assert not uncovered, (
        "pydantic declares free-form container position(s) the walk still treats "
        f"as schema: {sorted(uncovered)}. Add them to _NON_SCHEMA_KEYS, or justify "
        "each one in the comment above it."
    )


def test_the_function_position_is_safe_to_walk():
    """Why ``function`` is NOT in the exclusion list.

    Its annotation is ``Callable``, but the compiled node stores a dict
    (``{"type": "with-info", "function": <callable>}``) that the walk descends
    into as though it were a node — so it looks like a data-as-schema position
    and was a candidate for exclusion. It was left out: excluding a key means
    never walking its subtree, which is the one mistake here that fails OPEN, and
    this payload cannot be mistaken for anything. Dropping it into the list
    changed no test, and an entry no test can miss is an entry that buys nothing.
    """
    from pydantic import ConfigDict, field_validator, model_validator

    class Model(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        name: str = "x"

        @field_validator("name", mode="after")
        @classmethod
        def _f(cls, value: str) -> str:
            return value

        @model_validator(mode="before")
        @classmethod
        def _m(cls, data):
            return data

    seen = []
    stack = [Model.__pydantic_core_schema__]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            payload = node.get("function")
            if isinstance(payload, dict):
                seen.append(payload)
            stack.extend(v for v in node.values() if isinstance(v, (dict, list)))
        elif isinstance(node, list):
            stack.extend(v for v in node if isinstance(v, (dict, list)))

    assert seen, "no compiled `function` payload found — the shape has changed"
    for payload in seen:
        assert set(payload) <= {"type", "function", "field_name"}, payload
        assert payload.get("type") in ("with-info", "no-info"), payload
        assert not any(isinstance(v, (dict, list)) for v in payload.values()), payload


def test_resolve_core_ref_follows_a_reference_and_bounds_its_hops():
    """Direct cover for the ref resolver.

    It is documented as defensive — the generic descent into ``definitions``
    reaches the same node — so nothing in the suite exercised it and a mutant that
    removed it survived. Defensive is not the same as untested: a resolver that
    silently returned the wrong node would fail CLOSED, which is loud but wrong.
    """
    target = {"type": "model", "config": {"extra_fields_behavior": "forbid"}}
    definitions = {
        "a": {"type": "definition-ref", "schema_ref": "b"},
        "b": target,
    }

    # Follows a chain to the node it names.
    assert (
        registry_module._resolve_core_ref(
            {"type": "definition-ref", "schema_ref": "a"}, definitions
        )
        is target
    )

    # A cycle terminates instead of spinning, and returns a node (never None).
    cyclic = {"x": {"type": "definition-ref", "schema_ref": "y"}}
    cyclic["y"] = {"type": "definition-ref", "schema_ref": "x"}
    resolved = registry_module._resolve_core_ref(
        {"type": "definition-ref", "schema_ref": "x"}, cyclic, budget=4
    )
    assert isinstance(resolved, dict)

    # An unresolvable ref yields ``None`` rather than raising, and the walk drops
    # a non-dict. That is safe HERE only because the walk also descends into the
    # ``definitions`` array generically, so the target is reached by the other
    # route — pinned below rather than asserted in prose.
    dangling = {"type": "definition-ref", "schema_ref": "missing"}
    assert registry_module._resolve_core_ref(dangling, {}) is None


def test_a_recursive_open_model_is_still_rejected_when_its_ref_dead_ends():
    """The property that makes the resolver's ``None`` return safe.

    A recursive model parks itself in ``definitions`` behind a reference. If the
    walk depended on resolving that reference, a dead end would silently skip the
    model — fail-OPEN. It does not: the ``definitions`` array is walked directly,
    so the open model is caught either way (issue #145, live QA).
    """
    from typing import List, Optional

    from pydantic import ConfigDict

    class RecursiveOpen(RecipeInputBase):
        model_config = ConfigDict(extra="allow", frozen=True)
        name: str = "n"
        children: Optional[List["RecursiveOpen"]] = None

    RecursiveOpen.model_rebuild()

    with pytest.raises(ValueError, match="reject undeclared keys"):
        registry_module._check_input_model_forbids_extras("probe", RecursiveOpen)

    class RecursiveClosed(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        name: str = "n"
        children: Optional[List["RecursiveClosed"]] = None

    RecursiveClosed.model_rebuild()
    registry_module._check_input_model_forbids_extras("probe", RecursiveClosed)


def test_the_shared_walker_reaches_nested_and_sequence_positions():
    """Both compiled-schema gates depend on this one traversal, so it is pinned
    directly rather than only through the checks that use it."""
    from typing import Tuple as TupleType

    from pydantic import ConfigDict

    class Leaf(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        marker: str = "x"

    class Model(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        one: Leaf = Leaf()
        many: TupleType[Leaf, ...] = ()

    nodes = list(registry_module._iter_core_schema_nodes(Model))
    assert nodes, "the walker yielded nothing"
    # Every field container in the tree, including the ones below the root.
    field_names = set()
    for node in nodes:
        fields = node.get("fields")
        if isinstance(fields, dict):
            field_names.update(fields)
    assert {"one", "many", "marker"} <= field_names, sorted(field_names)


def test_an_input_model_that_cannot_rebuild_is_a_build_defect():
    """The schema gate rebuilds before reading, and a rebuild can fail.

    Rebuilding is what stops a swapped ``__pydantic_core_schema__`` from fooling
    the gate (issue #145, live QA #390) — so a model whose schema cannot be
    regenerated has no readable shape to gate at all, and that is a build defect
    rather than something to pass over silently.
    """
    from pydantic import ConfigDict

    from boomi_mcp.recipes.contracts import RecipeInputBase

    class UnrebuildableInputV1(RecipeInputBase):
        model_config = ConfigDict(extra="forbid", frozen=True)
        version: str = "1"

        @classmethod
        def model_rebuild(cls, **kwargs):
            raise RuntimeError("no schema for you")

    with pytest.raises(ValueError, match="cannot be rebuilt"):
        build_test_registry((_reg(input_model=UnrebuildableInputV1),))


def test_a_split_owner_registration_is_a_build_defect():
    """A model and executor from different packages break §7's own reasoning.

    §7 accepts that a registered model can hand its executor anything, because a
    model stashing the caller's mapping in a module global reaches it identically
    — a channel §12 already declares open. That holds only while the two are
    written together. Pair a hostile or buggy model with SOMEONE ELSE'S honest
    executor and it stops holding, because the stash needs the executor to
    cooperate and a payload in a declared field does not (issue #145).

    Asserted over the production catalogue, so the assumption cannot expire in
    silence.
    """
    from boomi_mcp.recipes.registry import _check_input_owner_shared

    _check_input_owner_shared(_reg())  # the shipped pairing is same-package

    def _foreign_executor(inp):  # pragma: no cover - rejected before it can run
        return ()

    _foreign_executor.__module__ = "some.other.package"
    with pytest.raises(ValueError, match="must ship in one package"):
        _check_input_owner_shared(_reg(executor=_foreign_executor))


def test_the_owner_check_runs_over_the_shipped_catalogue():
    """The assertion must be WIRED, not merely present.

    `_check_input_owner_shared` is called from `production_registry`; a test that
    only calls the function directly leaves the loop deletable with the whole
    suite green, which is the same gap that left a pin's only call site
    unobserved (issue #145, live QA #399).
    """
    import boomi_mcp.recipes.registry as registry_module
    from boomi_mcp.recipes.builtins.catalog import PRODUCTION_REGISTRATIONS

    expected = sum(
        1
        for reg in PRODUCTION_REGISTRATIONS
        if reg.input_model is not None and reg.executor is not None
    )
    assert expected > 0  # the premise: there is something to check

    seen = []
    original = registry_module._check_input_owner_shared
    registry_module._check_input_owner_shared = lambda reg: seen.append(reg.recipe_id)
    try:
        registry_module._PRODUCTION_REGISTRY = None
        registry_module.production_registry()
    finally:
        registry_module._check_input_owner_shared = original
        registry_module._PRODUCTION_REGISTRY = None
    assert len(seen) == expected, seen


def test_the_owner_check_compares_packages_not_modules():
    """Sibling modules in one package are one owner; separate packages are not.

    Every shipped pair happens to sit in the SAME module, so a comparison of full
    module names would pass the catalogue while rejecting a legitimate split
    across `pkg/models.py` and `pkg/executors.py` (issue #145, live QA #399).
    """
    from boomi_mcp.recipes.registry import _check_input_owner_shared

    def _sibling_executor(inp):  # pragma: no cover - never invoked
        return ()

    _sibling_executor.__module__ = "boomi_mcp.recipes.builtins.executors"
    _check_input_owner_shared(_reg(executor=_sibling_executor))  # same package: fine

    def _stranger(inp):  # pragma: no cover - never invoked
        return ()

    _stranger.__module__ = "boomi_mcp.other.package.executors"
    with pytest.raises(ValueError, match="must ship in one package"):
        _check_input_owner_shared(_reg(executor=_stranger))


def test_a_built_registry_refuses_further_registration():
    """"An immutable set of registered recipe versions" has to be true.

    ``_register`` stayed callable on the finished object, so a later call mutated
    the live mappings AFTER ``_registry_revision`` was computed and AFTER the
    constructor's cross-registration checks ran — leaving a registry whose
    revision no longer describes it. The existing surface test could not see this:
    it looks for ``register``/``add``/``install``/``unregister``/``clear``, and
    this method is ``_register`` (issue #145, §6 architect review).
    """
    registry = build_test_registry((_reg(),))
    revision_before = registry.registry_revision

    with pytest.raises(ValueError, match="sealed"):
        registry._register(_reg(recipe_id="test.other"), "rev")

    assert registry.registry_revision == revision_before
    # ...and the registration really did not land.
    with pytest.raises(Exception):
        registry.resolve("test.other", "1.0.0")


def _registry_call_offenders(sources):
    """Report any construction of a registry, or any ``registry=`` forwarding.

    ONE scanner, used both on the repository and on the test's own probe. The
    previous version had a separate mini-scanner inside its self-check, so the
    branches that matter — alias resolution, ``**kwargs`` smuggling — could be
    deleted from the real scanner while the self-check went on passing
    (issue #145, §6 architect review round 4).
    """
    import ast

    watched = {"RecipeRegistry", "build_test_registry", "run_recipes"}
    offenders = []
    for label, text in sources:
        tree = ast.parse(text)

        aliases = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for name in node.names:
                    if name.name in watched:
                        aliases[name.asname or name.name] = name.name
            elif isinstance(node, ast.Assign):
                value = node.value
                if isinstance(value, ast.Name) and value.id in aliases:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            aliases[target.id] = aliases[value.id]

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            local = func.id if isinstance(func, ast.Name) else (
                func.attr if isinstance(func, ast.Attribute) else None
            )
            canonical = aliases.get(local, local)
            if canonical in ("RecipeRegistry", "build_test_registry"):
                offenders.append(f"{label}:{node.lineno}: constructs {canonical}")
            elif canonical == "run_recipes" and any(
                kw.arg == "registry" or kw.arg is None for kw in node.keywords
            ):
                offenders.append(f"{label}:{node.lineno}: passes registry= to run_recipes")
    return offenders


def test_no_mcp_surface_constructs_or_accepts_a_registry():
    """The boundary claim is "no MCP-ACCESSIBLE registrar", so test THAT.

    Two earlier versions of this test were weaker than they looked. A regex could
    not cross a nested call, so ``run_recipes(make_requests(), registry=r)``
    slipped through. Then the AST version scanned only ``src/boomi_mcp`` — while
    the fifty ``@mcp.tool`` functions that ARE the MCP surface live in the root
    ``server.py``, so a registrar added there would have passed
    (issue #145, §6 architect review rounds 3 and 4).

    A tripwire that does not watch the door is worse than no tripwire.
    """
    sources = []
    server = _project_root / "server.py"
    assert server.is_file(), server
    server_text = server.read_text()
    # The premise: this file really is the MCP registration surface.
    assert server_text.count("@mcp.tool") > 10
    sources.append(("server.py", server_text))

    src = _project_root / "src" / "boomi_mcp"
    for path in sorted(src.rglob("*.py")):
        relative = path.relative_to(src).as_posix()
        if relative.startswith("recipes/"):
            continue  # the layer may name its own types
        sources.append((relative, path.read_text()))

    # ASSERT THE FILE SET, not only the verdict. A scan with no offenders passes
    # whether or not it opened the file that matters, so dropping ``server.py``
    # from ``sources`` would otherwise be invisible.
    scanned = [label for label, _ in sources]
    assert "server.py" in scanned, scanned
    assert any(label.startswith("categories/") for label in scanned), scanned[:10]

    assert _registry_call_offenders(sources) == [], _registry_call_offenders(sources)


def test_the_registry_reachability_scanner_sees_what_it_forbids():
    """Guard the guard, through the SAME function the repository scan uses."""
    probe = (
        "from boomi_mcp.recipes import RecipeRegistry as RR, run_recipes\n"
        "alias = RR\n"
        "r = alias(())\n"
        "run_recipes(make_requests(), registry=r)\n"
        "run_recipes(make_requests(), **{'registry': r})\n"
    )
    found = _registry_call_offenders([("probe.py", probe)])
    assert any("constructs RecipeRegistry" in item for item in found), found
    assert sum("passes registry=" in item for item in found) == 2, found

    # ...and it stays quiet on an honest call.
    assert _registry_call_offenders([("clean.py", "run_recipes(reqs, catalog=c)\n")]) == []


def test_a_built_registry_exposes_no_writable_mappings():
    """Blocking ``_register`` left the backing dictionaries plain and writable.

    A direct assignment still changed what the registry resolved, while
    ``registry_revision`` — computed during construction — went on describing
    something else (issue #145, §6 architect review).
    """
    registry = build_test_registry((_reg(),))
    for attr in ("_descriptors", "_executors", "_input_models", "_declared_shape"):
        mapping = getattr(registry, attr)
        with pytest.raises(TypeError):
            mapping["x"] = "y"  # type: ignore[index]

    # ...and the published view is read-only too.
    with pytest.raises(TypeError):
        registry.declared_shape()["x"] = {}  # type: ignore[index]

    # ``_defaults`` decides which VERSION a bare recipe id resolves to. Freezing
    # only the four descriptor maps left it writable, so moving it changed what
    # ran while ``registry_revision`` stayed byte-identical.
    revision = registry.registry_revision
    key = next(iter(registry._defaults))
    with pytest.raises(TypeError):
        registry._defaults[key] = "9.9.9"  # type: ignore[index]
    assert registry.registry_revision == revision

    # The NESTED annotation maps are the record the engine compares against.
    nested = next(iter(registry.declared_shape().values()))
    with pytest.raises(TypeError):
        nested["injected"] = None  # type: ignore[index]
