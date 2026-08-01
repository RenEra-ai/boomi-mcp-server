"""Strict-contract tests for the four recipe contributions (issue #145 M12.10).

Covers the authored boundary: the closed union and its DERIVED kind list, closed
operations, forbidden fields, repr redaction, canonical serialization, and the
pinned component-type copy.
"""

import json
import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models import recipe_contributions as rc
from boomi_mcp.models.recipe_contributions import (
    RECIPE_COMPONENT_TYPES,
    RECIPE_CONTRIBUTION_KINDS,
    RECIPE_CONTRIBUTION_VERSION,
    RECIPE_EXCLUDED_COMPONENT_TYPES,
    ComponentContributionV1,
    RecipeContributionValidationError,
    canonical_recipe_contribution_json,
    parse_recipe_contribution,
    recipe_contribution_v1_json_schema,
    scan_forbidden_recipe_shape,
    validate_contribution_object,
)

_ROOT = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "source",
                "connection_ref": "$ref:src_conn",
                "operation_ref": "$ref:src_op",
            },
            {"kind": "map_ref", "map_ref": "$ref:the_map"},
            {
                "kind": "target",
                "connection_ref": "$ref:tgt_conn",
                "operation_ref": "$ref:tgt_op",
            },
            {"kind": "stop"},
        ],
    },
}

_SAMPLES = {
    "process_ir_patch": {
        "contribution_kind": "process_ir_patch",
        "version": "1",
        "process_key": "main_process",
        "operations": [
            {
                "operation_id": "op.root",
                "op": "set_process_root",
                "slot": "root",
                "root": _ROOT,
            }
        ],
    },
    "system_topology_patch": {
        "contribution_kind": "system_topology_patch",
        "version": "1",
        "topology_id": "t.demo",
        "profile_ref": "profile-x",
        "operations": [
            {
                "operation_id": "op.obj",
                "op": "add_object",
                "object": {
                    "kind": "process",
                    "key": "p",
                    "component_ref": "$ref:main_process",
                },
            }
        ],
    },
    "component_contribution": {
        "contribution_kind": "component_contribution",
        "version": "1",
        "contribution_id": "c.0",
        "component_key": "main_process",
        "component_type": "process",
        "materialization_mode": "create",
        "materializer_slot": "slot.main_process",
    },
    "constraint_requirement": {
        "contribution_kind": "constraint_requirement",
        "version": "1",
        "requirement_id": "req.process",
        "requirement": {"kind": "process", "process_key": "main_process"},
    },
}


# ---------------------------------------------------------------------------
# The union and its DERIVED kind list
# ---------------------------------------------------------------------------


def test_sample_table_covers_every_declared_kind():
    """The samples ARE the corpus, so a gap in them is a coverage hole."""
    assert set(_SAMPLES) == set(RECIPE_CONTRIBUTION_KINDS)


def test_contribution_kinds_are_derived_from_the_union_not_hand_listed():
    """No copied count anywhere: the tuple is read back off the union members."""
    from typing import get_args

    members = get_args(get_args(rc.RecipeContributionV1)[0])
    derived = tuple(
        get_args(m.model_fields["contribution_kind"].annotation)[0] for m in members
    )
    assert derived == RECIPE_CONTRIBUTION_KINDS
    assert len(RECIPE_CONTRIBUTION_KINDS) == 4


@pytest.mark.parametrize("kind", sorted(_SAMPLES))
def test_every_kind_round_trips(kind):
    parsed = parse_recipe_contribution(_SAMPLES[kind])
    assert parsed.contribution_kind == kind
    assert parsed.version == RECIPE_CONTRIBUTION_VERSION


@pytest.mark.parametrize("kind", sorted(_SAMPLES))
def test_canonical_bytes_are_stable_across_key_order(kind):
    payload = _SAMPLES[kind]
    shuffled = dict(reversed(list(payload.items())))
    assert canonical_recipe_contribution_json(
        parse_recipe_contribution(payload)
    ) == canonical_recipe_contribution_json(parse_recipe_contribution(shuffled))


@pytest.mark.parametrize("kind", sorted(_SAMPLES))
def test_canonical_bytes_survive_a_reparse(kind):
    once = parse_recipe_contribution(_SAMPLES[kind])
    twice = parse_recipe_contribution(json.loads(canonical_recipe_contribution_json(once)))
    assert canonical_recipe_contribution_json(once) == canonical_recipe_contribution_json(
        twice
    )


def test_unknown_contribution_kind_is_reported_as_such():
    with pytest.raises(RecipeContributionValidationError) as exc:
        parse_recipe_contribution({"contribution_kind": "nope", "version": "1"})
    assert exc.value.diagnostics[0][1] == "/contribution_kind"


def test_missing_contribution_kind_is_reported_as_such():
    with pytest.raises(RecipeContributionValidationError) as exc:
        parse_recipe_contribution({"version": "1"})
    assert exc.value.diagnostics[0][1] == "/contribution_kind"


@pytest.mark.parametrize("kind", sorted(_SAMPLES))
def test_wrong_version_is_rejected(kind):
    payload = dict(_SAMPLES[kind], version="2")
    with pytest.raises(RecipeContributionValidationError) as exc:
        parse_recipe_contribution(payload)
    assert exc.value.diagnostics[0][1] == "/version"


def test_a_non_object_payload_is_rejected():
    for payload in ([], "x", 3, None):
        with pytest.raises(RecipeContributionValidationError):
            parse_recipe_contribution(payload)


# ---------------------------------------------------------------------------
# Closed operations
# ---------------------------------------------------------------------------


def test_unknown_process_operation_tag_is_rejected():
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    payload["operations"][0]["op"] = "replace_everything"
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


def test_unknown_topology_operation_tag_is_rejected():
    payload = json.loads(json.dumps(_SAMPLES["system_topology_patch"]))
    payload["operations"][0]["op"] = "remove_object"
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


def test_duplicate_operation_id_within_a_patch_is_rejected():
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    payload["operations"].append(json.loads(json.dumps(payload["operations"][0])))
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


def test_a_patch_with_no_operations_is_rejected():
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    payload["operations"] = []
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


def test_the_three_process_slots_are_the_only_ones():
    slots = set()
    from typing import get_args

    for member in get_args(get_args(rc.ProcessIRPatchOperationV1)[0]):
        slots |= set(get_args(member.model_fields["slot"].annotation))
    assert slots == {"root", "root.before_terminal", "root.terminal.branch.legs"}


def test_topology_operations_are_additive_only():
    """No update/replace/remove/lifecycle operation exists at all."""
    from typing import get_args

    ops = set()
    for member in get_args(get_args(rc.SystemTopologyPatchOperationV1)[0]):
        ops |= set(get_args(member.model_fields["op"].annotation))
    assert ops == {"add_object", "add_relation"}


# ---------------------------------------------------------------------------
# Forbidden shapes
# ---------------------------------------------------------------------------

_FORBIDDEN_CASES = [
    ("config", {"a": 1}),
    ("configuration", {"a": 1}),
    ("metadata", {"a": 1}),
    ("parameters", {"a": 1}),
    ("extensions", {"a": 1}),
    ("settings", {"host": "x"}),
    ("xml", "<Component/>"),
    ("raw_xml", "<Component/>"),
    ("component_xml", "<Component/>"),
    ("headers", {"X-Api-Key": "v"}),
    ("default_headers", {"a": "b"}),
    ("host", "db.example.invalid"),
    ("base_url", "https://x.invalid"),
    ("username", "svc"),
    ("sql", "SELECT 1"),
    ("credential_ref", "SECRET"),
    ("password", "hunter2"),
    ("api_key", "k"),
    ("auth_token", "t"),
    ("authorization", "Bearer x"),
    ("client_secret", "s"),
    ("code", "print(1)"),
    ("script", "x = 1"),
    ("script_body", "x = 1"),
    ("custom_scripting", {"script": "x"}),
    ("language", "groovy2"),
    ("module", "os"),
    ("path", "/body/steps/0"),
    ("json_pointer", "/body"),
    ("index", 0),
    ("node_id", "n1"),
    ("shape_id", "shape1"),
    ("edges", [{"from": "a", "to": "b"}]),
    ("depends_on", ["other_key"]),
    ("provenance", {"module": "x"}),
    ("implementation_sha256", "0" * 64),
    ("validation_policy", "flow_sequence"),
    ("waiver", True),
    ("passed", True),
    ("conflict_priority", 1),
]


@pytest.mark.parametrize("key,value", _FORBIDDEN_CASES, ids=[c[0] for c in _FORBIDDEN_CASES])
def test_forbidden_field_at_the_top_level_is_rejected(key, value):
    payload = dict(_SAMPLES["component_contribution"])
    payload[key] = value
    with pytest.raises(RecipeContributionValidationError) as exc:
        parse_recipe_contribution(payload)
    assert exc.value.diagnostics[0][1] == f"/{key}"


@pytest.mark.parametrize("key,value", _FORBIDDEN_CASES, ids=[c[0] for c in _FORBIDDEN_CASES])
def test_forbidden_field_nested_deep_inside_a_process_ir_is_rejected(key, value):
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    payload["operations"][0]["root"]["body"]["steps"][0][key] = value
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


def test_a_forbidden_key_with_an_empty_value_is_not_a_violation():
    """An absent optional field carries nothing; only a POPULATED one leaks."""
    for empty in (None, "", [], {}):
        assert scan_forbidden_recipe_shape({"config": empty}) is None


def test_custom_scripting_is_rejected_even_though_processir_admits_it():
    """The one place recipes are STRICTER than direct ProcessIR authoring.

    ``CustomScriptingOpV1`` is a real, supported ProcessIR node — but a recipe is
    code the registry vouches for, and vouching for arbitrary script text is what
    "no LLM-generated Python" rules out.
    """
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    ir = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    "connection_ref": "$ref:c",
                    "operation_ref": "$ref:o",
                },
                {
                    "kind": "data_process",
                    "steps": [
                        {
                            "operation": "custom_scripting",
                            "script": "x = 1",
                            "language": "groovy2",
                        }
                    ],
                },
                {"kind": "target", "connection_ref": "$ref:tc", "operation_ref": "$ref:to"},
                {"kind": "stop"},
            ],
        },
    }
    # Direct ProcessIR accepts it...
    parse_process_ir_v1(ir)
    # ...and the recipe layer does not.
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    payload["operations"][0]["root"] = ir
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


def test_an_unknown_field_is_rejected_by_the_strict_model():
    payload = dict(_SAMPLES["component_contribution"], surprise="x")
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


# ---------------------------------------------------------------------------
# The model_construct guard
# ---------------------------------------------------------------------------


def test_model_construct_cannot_bypass_field_validation():
    """``model_construct`` skips validation; the dump-and-reparse gate does not.

    Every field here IS declared, so it survives into ``model_dump()`` — which is
    exactly why dumping and reparsing is the guard and "it is already a model"
    is not. An untrimmed key, an excluded component type and a bogus mode all
    reach the gate and all fail it.
    """
    smuggled = ComponentContributionV1.model_construct(
        contribution_kind="component_contribution",
        version="1",
        contribution_id="c.0",
        component_key="  untrimmed  ",
        component_type="trading_partner",
        materialization_mode="whatever",
        materializer_slot="s",
    )
    with pytest.raises(RecipeContributionValidationError):
        validate_contribution_object(smuggled)


def test_model_construct_cannot_smuggle_an_undeclared_field():
    """The extra-field vector, pinned rather than assumed.

    Under ``extra="forbid"`` pydantic DROPS an undeclared kwarg in
    ``model_construct`` rather than storing it, so a forbidden bag never reaches
    the dump at all. Asserted here because "the scan would have caught it" and
    "it was never there" are different guarantees, and only one of them survives
    a pydantic upgrade — if a future version starts retaining extras, this test
    fails and the scan (already covering dumps) is what holds the line.
    """
    smuggled = ComponentContributionV1.model_construct(
        contribution_kind="component_contribution",
        version="1",
        contribution_id="c.0",
        component_key="k",
        component_type="process",
        materialization_mode="create",
        materializer_slot="s",
        config={"password": "hunter2"},
    )
    dumped = smuggled.model_dump(mode="json")
    assert "config" not in dumped
    assert scan_forbidden_recipe_shape(dumped) is None
    # And a dump that DID carry it is caught by the same gate.
    assert scan_forbidden_recipe_shape(
        dict(dumped, config={"password": "hunter2"})
    ) == (("config",), "forbidden_field")


def test_validate_contribution_object_rejects_a_non_model():
    for value in ({"a": 1}, "x", 3, None):
        with pytest.raises(RecipeContributionValidationError):
            validate_contribution_object(value)


def test_validate_contribution_object_rejects_a_foreign_model():
    from boomi_mcp.models.process_ir import ProcessIRV1

    with pytest.raises(RecipeContributionValidationError):
        validate_contribution_object(ProcessIRV1.model_validate(_ROOT))


# ---------------------------------------------------------------------------
# repr redaction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(_SAMPLES))
def test_repr_redacts_every_authored_value(kind):
    parsed = parse_recipe_contribution(_SAMPLES[kind])
    rendered = repr(parsed)
    for value in (
        "main_process",
        "slot.main_process",
        "$ref:src_conn",
        "profile-x",
        "t.demo",
        "req.process",
        "c.0",
    ):
        assert value not in rendered, (kind, value, rendered)


def test_repr_keeps_the_closed_discriminators_visible():
    """Redaction must not make a repr useless: the tags are safe and stay."""
    rendered = repr(parse_recipe_contribution(_SAMPLES["component_contribution"]))
    assert "component_contribution" in rendered
    assert "process" in rendered  # component_type
    assert "create" in rendered  # materialization_mode


# ---------------------------------------------------------------------------
# Schema strictness
# ---------------------------------------------------------------------------


def _walk_objects(node):
    if isinstance(node, dict):
        if node.get("type") == "object":
            yield node
        for value in node.values():
            yield from _walk_objects(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_objects(item)


def test_every_object_in_the_schema_is_closed():
    schema = recipe_contribution_v1_json_schema()
    for obj in _walk_objects(schema):
        assert obj.get("additionalProperties") is not True, obj.get("title")


def test_the_schema_carries_no_pointer_or_cfg_vocabulary():
    """A recipe addresses SEMANTIC slots; a pointer vocabulary would mean it
    could address structure, which is the generic patching this forbids."""
    blob = json.dumps(recipe_contribution_v1_json_schema(), sort_keys=True)
    for token in (
        "json_pointer",
        "cfg_node_id",
        "cfg_edge_id",
        "shape_id",
        "internal_node_id",
        "emitter_input",
    ):
        assert token not in blob, token


# ---------------------------------------------------------------------------
# The pinned component-type copy
# ---------------------------------------------------------------------------


def test_recipe_component_types_are_pinned_against_the_builder_authority():
    """A COPY, pinned in both directions — ``models/`` may not import ``categories/``.

    Same precedent as ProcessIR's copied secret list. The pin asserts the
    difference is exactly the DECLARED exclusion, so accidental drift shows up as
    a failure rather than as a quietly wider or narrower surface.
    """
    from boomi_mcp.categories.integration_builder import _COMPONENT_NAME_PRIMARY_TYPES

    authority = set(_COMPONENT_NAME_PRIMARY_TYPES) | {"process"}
    mine = set(RECIPE_COMPONENT_TYPES)
    assert mine <= authority, mine - authority
    assert authority - mine == set(RECIPE_EXCLUDED_COMPONENT_TYPES)


def test_recipe_component_types_are_sorted_and_unique():
    assert list(RECIPE_COMPONENT_TYPES) == sorted(set(RECIPE_COMPONENT_TYPES))


def test_the_component_type_literal_matches_the_tuple():
    from typing import get_args

    assert set(get_args(rc.RecipeComponentType)) == set(RECIPE_COMPONENT_TYPES)


def test_an_excluded_component_type_is_rejected():
    payload = dict(_SAMPLES["component_contribution"], component_type="trading_partner")
    with pytest.raises(RecipeContributionValidationError):
        parse_recipe_contribution(payload)


# ---------------------------------------------------------------------------
# Constraint requirements carry no self-discharge
# ---------------------------------------------------------------------------


def test_a_constraint_cannot_declare_itself_satisfied():
    """No ``passed``/``severity``/``waiver``/``exemption`` field exists to set."""
    for field in ("passed", "severity", "waiver", "exemption", "safe", "expression"):
        payload = dict(_SAMPLES["constraint_requirement"])
        payload[field] = True
        with pytest.raises(RecipeContributionValidationError):
            parse_recipe_contribution(payload)


def test_capability_requirements_admit_only_positive_states():
    from typing import get_args

    states = set(get_args(rc.RequireCapabilityV1.model_fields["required_state"].annotation))
    assert states == {"supported", "emittable", "plannable-only"}
    for negative in ("gated", "unsupported", "guidance-only", "absent"):
        payload = json.loads(json.dumps(_SAMPLES["constraint_requirement"]))
        payload["requirement"] = {
            "kind": "capability",
            "authority": "process_ir",
            "subject": "joins",
            "required_state": negative,
        }
        with pytest.raises(RecipeContributionValidationError):
            parse_recipe_contribution(payload)


def test_component_key_rejects_untrimmed_and_control_characters():
    for bad in (" k", "k ", "", "a\tb", "a\x00b"):
        payload = dict(_SAMPLES["component_contribution"], component_key=bad)
        with pytest.raises(RecipeContributionValidationError):
            parse_recipe_contribution(payload)


def test_semantic_ids_are_lower_case_and_bounded():
    for bad in ("Upper", "1leading", "has space", "a" * 129, ""):
        payload = dict(_SAMPLES["component_contribution"], contribution_id=bad)
        with pytest.raises(RecipeContributionValidationError):
            parse_recipe_contribution(payload)


# ---------------------------------------------------------------------------
# Codex-review regression (issue #145): pointers must resolve against the payload
# ---------------------------------------------------------------------------


def _diagnostic_paths(payload):
    try:
        parse_recipe_contribution(payload)
    except RecipeContributionValidationError as exc:
        return [path for _code, path, _reason in exc.diagnostics]
    raise AssertionError("expected a validation failure")


def _resolves(payload, pointer):
    """Whether an RFC 6901 pointer addresses something in the payload."""
    if pointer == "/":
        return True
    cursor = payload
    for raw in pointer.lstrip("/").split("/"):
        segment = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(cursor, dict):
            if segment not in cursor:
                return False
            cursor = cursor[segment]
        elif isinstance(cursor, list):
            if not segment.isdigit() or int(segment) >= len(cursor):
                return False
            cursor = cursor[int(segment)]
        else:
            return False
    return True


def test_a_union_tag_never_appears_in_a_diagnostic_pointer():
    """Pydantic v2 inserts the TAG VALUE into a discriminated-union location.

    A missing ``operation_ref`` was reported at
    ``/operations/0/set_process_root/root/body/steps/0/source/operation_ref``,
    where neither ``set_process_root`` nor ``source`` is a key in the payload —
    they are the ``op`` and ``kind`` values. The earlier filter dropped segments
    ending in ``]``, which is pydantic v1's spelling, so it matched nothing
    (issue #145, Codex review).
    """
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    del payload["operations"][0]["root"]["body"]["steps"][0]["operation_ref"]

    paths = _diagnostic_paths(payload)
    assert "/operations/0/root/body/steps/0/operation_ref" in paths
    for path in paths:
        assert "set_process_root" not in path, path
        assert "/source/" not in path, path


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda p: p["operations"][0]["root"]["body"]["steps"].__setitem__(
                1, {"kind": "target", "connection_ref": "$ref:t"}
            ),
            id="missing_key_in_a_nested_union_member",
        ),
        pytest.param(
            lambda p: p["operations"][0].__setitem__("slot", "not_a_slot"),
            id="bad_literal_on_the_operation",
        ),
        pytest.param(
            lambda p: p["operations"][0]["root"]["body"].__setitem__("steps", []),
            id="empty_sequence",
        ),
    ],
)
def test_every_diagnostic_pointer_resolves_against_the_submitted_payload(mutate):
    """The property the pointer CLAIMS — RFC 6901 against what was sent.

    The final segment of a ``missing`` diagnostic is the absent key itself, so it
    is allowed not to resolve; every segment before it must.
    """
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    mutate(payload)
    for path in _diagnostic_paths(payload):
        parent = path.rsplit("/", 1)[0] or "/"
        assert _resolves(payload, parent), (path, parent)


def test_a_tag_whose_value_equals_a_field_name_is_disambiguated():
    """``MapRefNodeV1`` is the one member whose tag VALUE equals a field NAME.

    ``{"kind": "map_ref", "map_ref": ...}`` — so a pydantic location can carry
    ``map_ref`` twice: first the union tag, then the field. Skipping greedily
    swallowed the field too, and a value-wise "is this the last segment" test
    kept both, yielding ``.../steps/1/map_ref/map_ref`` (issue #145, live QA).
    """
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    steps = payload["operations"][0]["root"]["body"]["steps"]
    steps.insert(1, {"kind": "map_ref", "map_ref": "$ref:m"})
    del steps[1]["map_ref"]

    paths = _diagnostic_paths(payload)
    assert "/operations/0/root/body/steps/1/map_ref" in paths
    assert not any(p.endswith("/map_ref/map_ref") for p in paths), paths


def test_a_present_tag_valued_field_is_still_addressable():
    """The converse: when the field IS present, the pointer must reach it.

    Disambiguation must not swallow a real key that happens to share the tag's
    spelling — otherwise the fix for one direction breaks the other.
    """
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    steps = payload["operations"][0]["root"]["body"]["steps"]
    steps.insert(1, {"kind": "map_ref", "map_ref": " leading-space-is-invalid"})

    paths = _diagnostic_paths(payload)
    assert "/operations/0/root/body/steps/1/map_ref" in paths


def test_the_discriminator_set_is_derived_not_hand_listed():
    """A hand-kept "all the members of X" list has been wrong here repeatedly.

    ``value_type`` (``PropertySourceV1`` / ``DecisionOperandV1``) was the miss:
    a ``set_ddp`` source-value error emitted a pointer that addressed nothing.
    The set is now walked out of the compiled core schema, so a sixth
    discriminator is picked up with no test to remember (issue #145, live QA).
    """
    from pydantic import TypeAdapter

    from boomi_mcp.models.recipe_contributions import (
        _DISCRIMINATOR_FIELDS,
        RecipeContributionV1,
    )

    found, seen = set(), set()

    def walk(node):
        if id(node) in seen:
            return
        seen.add(id(node))
        if isinstance(node, dict):
            if isinstance(node.get("discriminator"), str):
                found.add(node["discriminator"])
            for value in node.values():
                walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value)

    walk(TypeAdapter(RecipeContributionV1).core_schema)
    assert _DISCRIMINATOR_FIELDS == frozenset(found)
    # The one that was missing, named so a regression is legible.
    assert "value_type" in _DISCRIMINATOR_FIELDS
    assert len(_DISCRIMINATOR_FIELDS) == 5


def test_a_value_type_tag_does_not_corrupt_the_pointer():
    """The concrete miss: a ``set_ddp`` source-value error.

    A last-segment ``value_type`` tag was appended verbatim, yielding a pointer
    whose parent does not resolve.
    """
    payload = json.loads(json.dumps(_SAMPLES["process_ir_patch"]))
    steps = payload["operations"][0]["root"]["body"]["steps"]
    steps.insert(
        1, {"kind": "set_ddp", "name": "x", "source_values": [{"value_type": "ddp"}]}
    )

    paths = _diagnostic_paths(payload)
    for path in paths:
        assert "/ddp" not in path, path
        parent = path.rsplit("/", 1)[0] or "/"
        assert _resolves(payload, parent), (path, parent)
