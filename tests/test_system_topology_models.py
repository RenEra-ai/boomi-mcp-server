"""Strict-contract tests for SystemTopologySpecV1 (issue #144, M12.9).

Covers the authored boundary: round-tripping every kind, the closed
discriminated unions, forbidden-field rejection, diagnostic hygiene, document
rules, schema strictness, and byte-stable canonical serialization.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models import system_topology as st
from boomi_mcp.models.system_topology import (
    SYSTEM_TOPOLOGY_VERSION,
    TOPOLOGY_OBJECT_KINDS,
    TOPOLOGY_RELATION_KINDS,
    TOPOLOGY_RELATION_ROLES,
    SystemTopologySpecV1,
    SystemTopologyValidationError,
    canonical_system_topology_json,
    canonical_system_topology_schema_json,
    parse_system_topology_v1,
    system_topology_v1_json_schema,
)

_FIXTURES = _project_root / "tests" / "fixtures" / "system_topology"


def _fixture(name):
    return json.loads((_FIXTURES / name).read_text())


# One minimal valid instance per object kind, and one per relation kind, so the
# round-trip tests are DERIVED from the unions rather than a hand-kept list that
# silently stops covering a kind the day one is added.
_OBJECT_SAMPLES = {
    "process": {"kind": "process", "key": "p", "component_ref": "$ref:pk"},
    "api_service": {"kind": "api_service", "key": "a", "component_ref": "$ref:ak"},
    "document_cache": {"kind": "document_cache", "key": "c", "component_ref": "$ref:ck"},
    "process_property": {
        "kind": "process_property",
        "key": "pp",
        "component_ref": "$ref:ppk",
    },
    "runtime": {"kind": "runtime", "key": "rt", "runtime_ref": "runtime-1"},
    "environment": {
        "kind": "environment",
        "key": "e",
        "environment_ref": "env-1",
        "classification": "TEST",
    },
    "schedule": {"kind": "schedule", "key": "s"},
    "deployment_unit": {"kind": "deployment_unit", "key": "u"},
    "external_queue": {"kind": "external_queue", "key": "q", "resource_ref": "queue-1"},
    "external_event_stream": {
        "kind": "external_event_stream",
        "key": "es",
        "resource_ref": "stream-1",
    },
}

_RELATION_SAMPLES = {
    "process_call": {
        "kind": "process_call",
        "key": "r",
        "caller_process": "p",
        "callee_process": "p2",
    },
    "api_service_route": {
        "kind": "api_service_route",
        "key": "r",
        "api_service": "a",
        "listener_process": "p",
    },
    "document_cache_use": {
        "kind": "document_cache_use",
        "key": "r",
        "process": "p",
        "document_cache": "c",
    },
    "process_property_use": {
        "kind": "process_property_use",
        "key": "r",
        "process": "p",
        "process_property": "pp",
    },
    "schedule_binding": {
        "kind": "schedule_binding",
        "key": "r",
        "schedule": "s",
        "process": "p",
        "runtime": "rt",
    },
    "deployment_binding": {
        "kind": "deployment_binding",
        "key": "r",
        "deployment_unit": "u",
        "process": "p",
        "environment": "e",
    },
    "queue_reference": {
        "kind": "queue_reference",
        "key": "r",
        "process": "p",
        "external_queue": "q",
    },
    "event_stream_reference": {
        "kind": "event_stream_reference",
        "key": "r",
        "process": "p",
        "external_event_stream": "es",
    },
}


def _doc(objects, relations=()):
    return {
        "version": "1",
        "profile_ref": "profile-x",
        "objects": list(objects),
        "relations": list(relations),
    }


# ---------------------------------------------------------------------------
# Union coverage and round-tripping
# ---------------------------------------------------------------------------


def test_object_samples_cover_every_declared_kind():
    """The sample table is the test corpus, so a gap in it is a coverage hole."""
    assert set(_OBJECT_SAMPLES) == set(TOPOLOGY_OBJECT_KINDS)


def test_relation_samples_cover_every_declared_kind():
    assert set(_RELATION_SAMPLES) == set(TOPOLOGY_RELATION_KINDS)


@pytest.mark.parametrize("kind", sorted(_OBJECT_SAMPLES))
def test_every_object_kind_round_trips(kind):
    sample = _OBJECT_SAMPLES[kind]
    # A schedule/deployment_unit needs its binding to exist at all, so those two
    # are exercised through the full fixture rather than standalone.
    if kind in ("schedule", "deployment_unit"):
        pytest.skip("identity is supplied by a binding; covered by the bound tests")
    spec = parse_system_topology_v1(_doc([sample]))
    assert spec.objects[0].kind == kind
    assert json.loads(canonical_system_topology_json(spec))["objects"][0]["kind"] == kind


@pytest.mark.parametrize("kind", sorted(_RELATION_SAMPLES))
def test_every_relation_kind_round_trips(kind):
    relation = dict(_RELATION_SAMPLES[kind])
    objects = [
        _OBJECT_SAMPLES["process"],
        {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
        _OBJECT_SAMPLES["api_service"],
        _OBJECT_SAMPLES["document_cache"],
        _OBJECT_SAMPLES["process_property"],
        _OBJECT_SAMPLES["runtime"],
        _OBJECT_SAMPLES["environment"],
        _OBJECT_SAMPLES["schedule"],
        _OBJECT_SAMPLES["deployment_unit"],
        _OBJECT_SAMPLES["external_queue"],
        _OBJECT_SAMPLES["external_event_stream"],
    ]
    # Every schedule/unit must be bound, so the two bindings ride along.
    relations = [relation]
    if kind != "schedule_binding":
        relations.append(_RELATION_SAMPLES["schedule_binding"] | {"key": "rs"})
    if kind != "deployment_binding":
        relations.append(_RELATION_SAMPLES["deployment_binding"] | {"key": "rd"})
    spec = parse_system_topology_v1(_doc(objects, relations))
    assert relation["kind"] in {rel.kind for rel in spec.relations}


def test_relation_roles_are_derived_from_the_models():
    """The role map must equal the models' own field lists.

    Derived, not hand-listed: the endpoint matrix and the semantic-duplicate key
    both consume this, and a stale copy would silently stop checking a role.
    """
    for kind, sample in _RELATION_SAMPLES.items():
        expected = tuple(k for k in sample if k not in ("kind", "key"))
        assert TOPOLOGY_RELATION_ROLES[kind] == expected, kind


# ---------------------------------------------------------------------------
# Strictness
# ---------------------------------------------------------------------------


def test_unknown_object_kind_is_reported_as_such():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(_doc([{"kind": "nope", "key": "k"}]))
    codes = {d.code for d in exc.value.diagnostics}
    assert codes == {"TOPOLOGY_SCHEMA_UNKNOWN_OBJECT"}


def test_unknown_relation_kind_is_reported_as_such():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(
            _doc([_OBJECT_SAMPLES["process"]], [{"kind": "nope", "key": "r"}])
        )
    codes = {d.code for d in exc.value.diagnostics}
    assert codes == {"TOPOLOGY_SCHEMA_UNKNOWN_RELATION"}


def test_missing_discriminator_is_an_unknown_object():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(_doc([{"key": "k", "component_ref": "x"}]))
    assert {d.code for d in exc.value.diagnostics} == {
        "TOPOLOGY_SCHEMA_UNKNOWN_OBJECT"
    }


def test_unsupported_version_is_rejected_before_model_validation():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(
            {"version": "2", "profile_ref": "p", "objects": [], "relations": []}
        )
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED"]
    assert diagnostics[0].path == "/version"


def test_empty_objects_list_is_a_cardinality_error():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(_doc([]))
    assert {d.code for d in exc.value.diagnostics} == {
        "TOPOLOGY_SCHEMA_INVALID_CARDINALITY"
    }


def test_non_object_payload_is_rejected():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(["not", "a", "document"])
    assert {d.code for d in exc.value.diagnostics} == {"TOPOLOGY_SCHEMA_INVALID"}


def test_extra_field_on_a_nested_object_is_rejected_with_its_path():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(
            _doc([dict(_OBJECT_SAMPLES["process"], surprise="x")])
        )
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_UNKNOWN_FIELD"]
    # The tag element pydantic inserts must be stripped: /objects/0/process/... is
    # a path that does not exist in the authored JSON.
    assert diagnostics[0].path == "/objects/0/surprise"


def test_extra_field_at_the_root_is_rejected():
    payload = _doc([_OBJECT_SAMPLES["process"]])
    payload["surprise"] = 1
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    assert {d.code for d in exc.value.diagnostics} == {"TOPOLOGY_SCHEMA_UNKNOWN_FIELD"}


@pytest.mark.parametrize(
    "bad_ref",
    ["", " leading", "trailing ", "$ref:", "$ref:has space", "with\nnewline", "nul\x00"],
)
def test_malformed_component_reference_is_rejected(bad_ref):
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(
            _doc([{"kind": "process", "key": "p", "component_ref": bad_ref}])
        )
    assert {d.code for d in exc.value.diagnostics} == {"TOPOLOGY_SCHEMA_INVALID"}


def test_component_reference_rule_matches_process_ir_exactly():
    """The copied rule must behave identically to the one it mirrors.

    Copied rather than imported (models must not depend on each other), so
    equality is pinned rather than assumed.
    """
    from boomi_mcp.models import process_ir

    for value in ("$ref:key", "literal-id", "a"):
        assert process_ir._validate_component_ref(value) == value
        assert st._validate_component_ref(value) == value
    for value in ("", " x", "x ", "$ref:", "$ref:a b"):
        with pytest.raises(Exception):
            process_ir._validate_component_ref(value)
        with pytest.raises(Exception):
            st._validate_component_ref(value)


def test_secret_substring_list_shares_the_process_ir_prefix():
    """The secret list is a COPY; drift between the two is the failure mode."""
    from boomi_mcp.models import process_ir

    assert st._SECRET_KEY_SUBSTRINGS == process_ir._FORBIDDEN_SECRET_KEY_SUBSTRINGS


# ---------------------------------------------------------------------------
# Forbidden fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "client_secret",
        "api_key",
        "auth_token",
        "authorization",
        "credentials",
        "bearer",
        "certificate",
        "environment_extension",
        "profile_override",
        "connection_properties",
    ],
)
def test_secret_shaped_keys_are_rejected_anywhere(key):
    payload = _doc([dict(_OBJECT_SAMPLES["process"], **{key: "value"})])
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_UNKNOWN_FIELD"]
    assert diagnostics[0].path == f"/objects/0/{key}"


@pytest.mark.parametrize(
    "key",
    ["config", "configuration", "metadata", "xml", "raw_xml", "component_xml", "extensions"],
)
def test_open_payload_keys_are_rejected_even_when_empty(key):
    """The objection to ``config: {}`` is the field's existence, not its contents."""
    payload = _doc([dict(_OBJECT_SAMPLES["process"], **{key: {}})])
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    assert [d.code for d in exc.value.diagnostics] == ["TOPOLOGY_SCHEMA_UNKNOWN_FIELD"]


@pytest.mark.parametrize(
    "key", ["capability", "provenance", "evidence", "action", "apply"]
)
def test_derived_verdict_fields_cannot_be_authored(key):
    """A payload may not assert its own capability state.

    This is the gate's whole premise: evidence decides, not the caller. If a
    spec could carry ``capability: emittable``, the registry would be advisory.
    """
    payload = _doc([dict(_OBJECT_SAMPLES["process"], **{key: "emittable"})])
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    assert [d.code for d in exc.value.diagnostics] == ["TOPOLOGY_SCHEMA_UNKNOWN_FIELD"]


def test_a_secret_nested_deep_in_the_document_is_still_found():
    payload = _doc(
        [_OBJECT_SAMPLES["process"]],
        [dict(_RELATION_SAMPLES["process_call"], nested={"inner": {"token": "abc"}})],
    )
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_UNKNOWN_FIELD"]
    assert diagnostics[0].path == "/relations/0/nested/inner/token"


def test_an_empty_secret_value_is_not_flagged():
    """Same value-shape rule as ProcessIR: an empty placeholder is not a leak."""
    payload = _doc([dict(_OBJECT_SAMPLES["process"], token="")])
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    # It is still rejected — but as an unknown FIELD by the strict model, which
    # is a different (and correct) reason from "you leaked a secret".
    assert [d.code for d in exc.value.diagnostics] == ["TOPOLOGY_SCHEMA_UNKNOWN_FIELD"]


# ---------------------------------------------------------------------------
# Diagnostic hygiene
# ---------------------------------------------------------------------------


def test_diagnostics_never_echo_an_authored_value():
    secret = "s3cr3t-value-that-must-not-appear"
    payload = _doc(
        [{"kind": "process", "key": secret, "component_ref": secret, "config": {}}]
    )
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    blob = repr(exc.value) + str(exc.value) + repr(exc.value.diagnostics)
    assert secret not in blob


def test_repr_of_a_model_suppresses_authored_values():
    spec = parse_system_topology_v1(_doc([_OBJECT_SAMPLES["process"]]))
    text = repr(spec.objects[0])
    assert "process" in text  # the discriminator IS structural
    assert "$ref:pk" not in text
    assert "..." in text


def test_diagnostics_are_sorted_and_stable():
    payload = _doc(
        [
            dict(_OBJECT_SAMPLES["process"], zeta="x"),
            {"kind": "runtime", "key": "rt", "runtime_ref": "r", "alpha": "y"},
        ]
    )
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    paths = [d.path for d in exc.value.diagnostics]
    assert paths == sorted(paths)


def test_every_diagnostic_carries_static_message_and_remediation():
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(_doc([{"kind": "nope", "key": "k"}]))
    for diagnostic in exc.value.diagnostics:
        assert diagnostic.message
        assert diagnostic.remediation
        assert diagnostic.message == st._MESSAGES[diagnostic.code]
        assert diagnostic.remediation == st._REMEDIATION[diagnostic.code]


def test_pointer_escaping_follows_rfc_6901():
    assert st._json_pointer(("a/b",)) == "/a~1b"
    assert st._json_pointer(("a~b",)) == "/a~0b"


# ---------------------------------------------------------------------------
# Document rules
# ---------------------------------------------------------------------------


def test_duplicate_object_key_points_at_the_later_occurrence():
    payload = _doc(
        [
            _OBJECT_SAMPLES["process"],
            {"kind": "process", "key": "p", "component_ref": "$ref:other"},
        ]
    )
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_DUPLICATE_KEY"]
    assert diagnostics[0].path == "/objects/1/key"


def test_duplicate_relation_key_is_rejected():
    objects = [
        _OBJECT_SAMPLES["process"],
        {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
        {"kind": "process", "key": "p3", "component_ref": "$ref:p3k"},
    ]
    relations = [
        {"kind": "process_call", "key": "r", "caller_process": "p", "callee_process": "p2"},
        {"kind": "process_call", "key": "r", "caller_process": "p", "callee_process": "p3"},
    ]
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(_doc(objects, relations))
    assert "TOPOLOGY_SCHEMA_DUPLICATE_KEY" in {
        d.code for d in exc.value.diagnostics
    }


def test_duplicate_semantic_relation_is_rejected_even_with_distinct_keys():
    """Two keys, one edge. Otherwise it would double-count in the runtime graph."""
    objects = [
        _OBJECT_SAMPLES["process"],
        {"kind": "process", "key": "p2", "component_ref": "$ref:p2k"},
    ]
    relations = [
        {"kind": "process_call", "key": "r1", "caller_process": "p", "callee_process": "p2"},
        {"kind": "process_call", "key": "r2", "caller_process": "p", "callee_process": "p2"},
    ]
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(_doc(objects, relations))
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_DUPLICATE_KEY"]
    assert diagnostics[0].path == "/relations/1"


def test_unbound_schedule_is_a_cardinality_error():
    """A schedule's identity IS its binding; unbound, it does not exist."""
    payload = _doc([_OBJECT_SAMPLES["process"], _OBJECT_SAMPLES["schedule"]])
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    diagnostics = exc.value.diagnostics
    assert [d.code for d in diagnostics] == ["TOPOLOGY_SCHEMA_INVALID_CARDINALITY"]
    assert diagnostics[0].path == "/objects/1"


def test_unbound_deployment_unit_is_a_cardinality_error():
    payload = _doc([_OBJECT_SAMPLES["process"], _OBJECT_SAMPLES["deployment_unit"]])
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    assert [d.code for d in exc.value.diagnostics] == [
        "TOPOLOGY_SCHEMA_INVALID_CARDINALITY"
    ]


def test_document_rule_findings_accumulate():
    """A caller fixes everything in one pass, not one round-trip at a time."""
    payload = _doc(
        [
            _OBJECT_SAMPLES["process"],
            {"kind": "process", "key": "p", "component_ref": "$ref:dup"},
            _OBJECT_SAMPLES["schedule"],
            _OBJECT_SAMPLES["deployment_unit"],
        ]
    )
    with pytest.raises(SystemTopologyValidationError) as exc:
        parse_system_topology_v1(payload)
    assert len(exc.value.diagnostics) == 3


# ---------------------------------------------------------------------------
# JSON Schema
# ---------------------------------------------------------------------------


def _walk_schema_objects(node):
    if isinstance(node, dict):
        if node.get("type") == "object" and "properties" in node:
            yield node
        for value in node.values():
            yield from _walk_schema_objects(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_schema_objects(item)


def test_every_schema_object_rejects_extras():
    for node in _walk_schema_objects(system_topology_v1_json_schema()):
        assert node.get("additionalProperties") is False


def test_schema_declares_closed_discriminated_unions():
    schema = system_topology_v1_json_schema()
    text = json.dumps(schema)
    assert '"discriminator"' in text
    assert '"propertyName": "kind"' in text
    for kind in TOPOLOGY_OBJECT_KINDS + TOPOLOGY_RELATION_KINDS:
        assert f'"{kind}"' in text, kind


def test_schema_carries_no_open_dictionary():
    """No arbitrary metadata/config dictionaries — an acceptance criterion.

    Checked structurally: every object node must close extras, and no node may
    declare a free-form ``additionalProperties`` schema (which is how a typed
    map slips in looking like a typed field).
    """
    def walk(node):
        if isinstance(node, dict):
            if "additionalProperties" in node:
                assert node["additionalProperties"] is False, node.get("title")
            if node.get("type") == "object" and "properties" not in node:
                raise AssertionError("an object node with no declared properties")
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(system_topology_v1_json_schema())


def _schema_vocabulary(schema):
    """Every FIELD NAME and closed-value literal the schema declares.

    Deliberately not a substring scan of the serialized schema: ``description``
    text is static documentation, and several descriptions legitimately name the
    very things the contract excludes — the schedule model explains that it
    carries no cron body precisely so a reader knows the omission is intentional.
    A blob scan would make writing that sentence a test failure, which is an
    incentive to delete the explanation rather than to keep the field out.

    The claim worth pinning is about the CONTRACT: no field is named ``config``,
    no enum admits ``cron``. That is what this collects.
    """
    names = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "properties" and isinstance(value, dict):
                    names.update(str(k).lower() for k in value)
                elif key in ("const", "enum"):
                    values = value if isinstance(value, list) else [value]
                    names.update(str(v).lower() for v in values if isinstance(v, str))
                elif key == "required" and isinstance(value, list):
                    names.update(str(v).lower() for v in value)
                if key != "description":
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(schema)
    return names


def test_schema_carries_no_cfg_layout_or_build_vocabulary():
    """The three-graph separation, checked at the vocabulary level.

    A topology document cannot name a ComponentPlan build dependency
    (``depends_on``) or a ProcessIR CFG/layout concept, because no such field
    exists to name.
    """
    vocabulary = _schema_vocabulary(system_topology_v1_json_schema())
    for forbidden in (
        "depends_on",
        "edge_id",
        "source_node_id",
        "target_node_id",
        "cfg_node_id",
        "dragpoint",
        "layout",
        "shape_id",
        "emitter_kind",
        "process_kind",
        "steps",
        "body",
    ):
        assert forbidden not in vocabulary, forbidden


def test_schema_carries_no_secret_or_executable_vocabulary():
    vocabulary = _schema_vocabulary(system_topology_v1_json_schema())
    for forbidden in (
        "password",
        "credentials",
        "certificate",
        "confirm_write",
        "action",
        "apply",
        "raw_xml",
        "xml",
        "config",
        "metadata",
        "capability",
        "provenance",
        "evidence",
    ):
        assert forbidden not in vocabulary, forbidden


def test_schema_has_no_schedule_content_or_account_limit_fields():
    """Both are deliberately unmodeled — no live evidence supports either.

    Every live schedule observed carried an empty body, and no account-limit
    capture exists at all, so a field for either would be an invention wearing
    the costume of evidence.
    """
    vocabulary = _schema_vocabulary(system_topology_v1_json_schema())
    for forbidden in (
        "cron",
        "interval",
        "max_retry",
        "retry",
        "active",
        "schedules",
        "limit",
        "quota",
        "license",
    ):
        assert forbidden not in vocabulary, forbidden


def test_schema_vocabulary_helper_actually_sees_real_fields():
    """Positive control: a checker that finds nothing would pass every test above."""
    vocabulary = _schema_vocabulary(system_topology_v1_json_schema())
    for expected in ("kind", "key", "component_ref", "profile_ref", "process_call"):
        assert expected in vocabulary, expected


# ---------------------------------------------------------------------------
# Canonical serialization + goldens
# ---------------------------------------------------------------------------


def test_canonical_json_is_stable_within_a_run():
    spec = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    assert canonical_system_topology_json(spec) == canonical_system_topology_json(spec)


def test_canonical_spec_golden_pin():
    """BYTE equality against a committed canonical form.

    Comparing shape — version, profile and two counts — is not a golden: it
    passes when a field's serialization changes, when a default stops being
    expanded, and when key ordering shifts. Those are exactly the regressions a
    canonical-serialization contract exists to catch.
    """
    payload = _fixture("system_topology_v1.json")
    spec = parse_system_topology_v1(payload)
    committed = (_FIXTURES / "system_topology_v1.canonical.json").read_text().strip()
    assert canonical_system_topology_json(spec) == committed

    # And the round trip preserves what was authored.
    produced = json.loads(canonical_system_topology_json(spec))
    assert produced["version"] == SYSTEM_TOPOLOGY_VERSION
    assert produced["profile_ref"] == payload["profile_ref"]
    assert len(produced["objects"]) == len(payload["objects"])
    assert len(produced["relations"]) == len(payload["relations"])


def test_schema_golden_pin_matches_the_committed_fixture():
    committed = (_FIXTURES / "system_topology_v1.schema.json").read_text()
    assert canonical_system_topology_schema_json() == committed.strip()


#: Present-tense universals about the live account. A rendered schema
#: description carries no provenance and no revision stamp, so a reader cannot
#: check one and a later account state silently falsifies it. Statements about
#: what a CAPTURE observed stay true; statements about what the account IS do
#: not. Same rule the plan's decisions and guidance already follow.
#: What this guard is, stated plainly so nobody mistakes it for more.
#:
#: It is a VOCABULARY BLOCKLIST, not a scope discriminator. It catches the
#: phrasings this contract has actually shipped and retracted; a universal that
#: avoids the words *live* and *zero* passes it, and some correct capture-scoped
#: prose trips it. Both were measured, not assumed.
#:
#: Widening it does not fix that — the class "present-tense claim about the
#: account" is not decidable from a word list, and every added pattern enlarges
#: an unmeasured false-positive surface that a future author will route around
#: by rewording, which is exactly how the class kept reappearing. What it buys
#: is real and bounded: no sentence this project has already got wrong can come
#: back on any of the six published surfaces, which is the failure mode that
#: actually recurred nine times.
#:
#: The durable rule is editorial, and the guard only enforces its cheapest half:
#: say what a CAPTURE OBSERVED, never what the account IS.
_LIVE_UNIVERSAL_PATTERNS = (
    r"\bthe live account\b",
    r"\bevery live\b",
    # Any count of "live profiles" at all. The account has ONE profile; every
    # phrasing that presumes two has been retracted, and "one of the two
    # live profiles" slipped past a pattern written for "in either/both".
    r"\blive profiles?\b",
    r"\bexists? in .{0,30}live\b",
    # Absolute evidence denials, which have twice been false of one member of
    # the very list they quantified over. ``no evidence to model from`` and
    # ``no evidence you can supply will clear it`` are exempt by construction:
    # the first is already capture-scoped by its sentence, the second is a
    # statement about the GATE, not about the account.
    r"\bno (live|current|available) evidence\b",
    r"\bno evidence at all\b",
    r"\bzero evidence\b",
    # Bare existence counts about the account, the shape #260 and the queue
    # bullet both used.
    r"\bzero \w+ components? (exist|are)\b",
    r"\bcontains zero\b",
)


def _schema_descriptions():
    """Every ``description`` the rendered schema publishes, with its path."""
    import json

    found = []

    def walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "description" and isinstance(value, str):
                    found.append((path, value))
                else:
                    walk(value, f"{path}/{key}")
        elif isinstance(node, list):
            for index, item in enumerate(node):
                walk(item, f"{path}/{index}")

    walk(json.loads(canonical_system_topology_schema_json()), "")
    return found


def _live_universals(descriptions):
    import re

    offenders = []
    for path, text in descriptions:
        normalized = re.sub(r"\s+", " ", text).strip().lower()
        for pattern in _LIVE_UNIVERSAL_PATTERNS:
            if re.search(pattern, normalized):
                offenders.append((path, pattern))
    return offenders


#: Deliberate quote-and-refute PHRASES: prose that names a retracted claim in
#: order to say it was wrong. Exempted by phrase, never by module — a
#: module-granular exemption hid a genuine live universal four items away from
#: the citation it was granted for, in the same docstring (QA #266). Each is
#: asserted to still be present, so the record of why cannot quietly vanish.
#: Bare phrases, quote-agnostic: the citation is wrapped in curly quotes and
#: split across source lines, so a literal with straight quotes matched nothing
#: — the QA #212 failure mode exactly. Stripped from NORMALIZED text.
_QUOTE_AND_REFUTE_PHRASES = ("every live deployment record is inactive",)


def _unwrap_callable(member):
    """The underlying function behind a method, property or decorator wrapper.

    ``vars(cls)`` hands back the DESCRIPTOR, not the function: a ``classmethod``
    object, a ``property``, a pydantic validator wrapper. Reading ``__doc__``
    off the descriptor found three of the five method docstrings in one module
    and silently skipped the other two — the same "the enumeration missed a kind
    of thing" hole as the package-vs-children one, one level further in.
    """
    for attribute in ("__func__", "fget", "func", "wrapped", "wrapped_property"):
        inner = getattr(member, attribute, None)
        if inner is not None and inner is not member:
            return _unwrap_callable(inner)
    return member


def _package_docstring_count():
    """Every docstring in the package's SOURCE, counted by AST.

    An independent ledger, so "the census reaches every docstring" is a checked
    property rather than a magic number that drifts. Two consecutive rounds of
    findings were the census missing a kind of definition; a hand-maintained
    floor cannot notice a third.
    """
    import ast
    import pathlib

    import boomi_mcp.compiler.system_topology as pkg
    import boomi_mcp.models.system_topology as models

    # rglob, not glob: the two ledgers cannot drift FROM each other, but with a
    # hard-coded flat file list they drift TOGETHER the moment a subpackage
    # appears — and this equality exists precisely to survive future additions,
    # in an epic whose history is module extraction.
    files = sorted(pathlib.Path(pkg.__path__[0]).rglob("*.py"))
    files.append(pathlib.Path(models.__file__))
    total = 0
    for path in files:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(
                node,
                (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            ) and ast.get_docstring(node):
                total += 1
    return total


def _emitted_strings(function_name, keyword):
    """Every literal ``message=``/``question=`` the named deriver can construct.

    Keyed on the STRING, not on the subject name. A subject-name ledger is
    blind at exactly one notch: a second construction site reusing a subject
    already reached from elsewhere adds no new name, so its text was never
    censused — QA #275's own argument one level in. The string is a fixed
    point, because the string is what a caller reads.

    Every site must supply that keyword as a literal. A ``**kwargs`` splat
    carries ``ast.keyword.arg is None`` and would otherwise contribute nothing
    silently, so it is asserted rather than skipped; the same assert makes a
    computed message loud instead of invisible.
    """
    import ast
    import inspect
    import textwrap

    from boomi_mcp.compiler.system_topology import relations

    tree = ast.parse(
        textwrap.dedent(inspect.getsource(getattr(relations, function_name)))
    )
    constructors = {"TopologyGuidanceV1", "TopologyDecisionV1"}
    strings, sites = set(), 0
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in constructors
        ):
            continue
        sites += 1
        values = [kw.value for kw in node.keywords if kw.arg == keyword]
        assert len(values) == 1, (function_name, keyword, "site without one literal")
        assert isinstance(values[0], ast.Constant), (
            function_name,
            ast.dump(values[0]),
        )
        strings.add(_normalize_prose(values[0].value))
    assert sites and len(strings) == sites, (function_name, sites, len(strings))
    return strings


def _normalize_prose(text):
    import re

    return re.sub(r"\s+", " ", text).strip().lower()


def _all_published_prose():
    """Every string this package puts in front of a caller, with its origin.

    SIX surfaces. Each earlier version of this guard read a subset, and each
    time the next defect shipped on a surface it did not read — including,
    twice, the surface the motivating defect had itself shipped on. Guidance and
    decisions are collected by INVOKING the derivers, not by reading their
    source: source carries the comments that quote retracted claims in order to
    refute them, and a guard that reads those is guaranteed to fire on its own
    explanations. They are invoked over BOTH context arms, because one spec is
    not enough — ``derive_unresolved_decisions`` branches on the snapshot, and a
    snapshot-free call reaches only half its strings.
    """
    import importlib
    import inspect
    import pkgutil

    import boomi_mcp.compiler.system_topology as pkg
    import boomi_mcp.models.system_topology as models
    from boomi_mcp.compiler.system_topology import findings as findings_mod
    from boomi_mcp.compiler.system_topology.context import (
        DependencyCorroborationV1,
        TopologyResolutionContextV1,
        prepare_topology_context,
    )
    from boomi_mcp.compiler.system_topology.relations import (
        derive_guidance,
        derive_unresolved_decisions,
    )

    # Prefixed so the per-surface tally below can tell schema descriptions
    # from the class docstrings that are NOT rendered into the schema.
    rows = [("schema" + path, text) for path, text in _schema_descriptions()]

    # ``pkg`` itself is listed explicitly: ``iter_modules`` yields a package's
    # CHILDREN, never the package, so the front-door ``__init__`` docstring —
    # the first thing a reader opens — was the one module of thirteen this
    # guard did not read.
    # ``walk_packages`` recurses, so a future subpackage is censused rather than
    # silently dropped from both sides at once (QA #274).
    modules = [models, pkg] + [
        importlib.import_module(info.name)
        for info in pkgutil.walk_packages(pkg.__path__, prefix=f"{pkg.__name__}.")
    ]
    for module in modules:
        short = module.__name__.rsplit(".", 1)[-1]
        if module.__doc__:
            rows.append((f"module:{short}", module.__doc__))
        # Class and function docstrings too: only a fraction of the package's
        # models are rendered into the schema, and one of the unrendered ones
        # was carrying a live universal.
        for name, obj in vars(module).items():
            if (
                (inspect.isclass(obj) or inspect.isfunction(obj))
                and getattr(obj, "__module__", None) == module.__name__
                and obj.__doc__
            ):
                rows.append((f"{short}.{name}", obj.__doc__))
            # ...and the class's own MEMBERS. ``vars(module)`` yields the class,
            # never its methods, so five docstrings — including a
            # ``@computed_field`` property whose text a caller reads straight
            # off the schema-adjacent contract — were outside the census. Same
            # shape as the package-vs-children hole, one level further in.
            if inspect.isclass(obj) and getattr(obj, "__module__", None) == module.__name__:
                for member_name, member in vars(obj).items():
                    if member_name.startswith("__"):
                        continue
                    target = _unwrap_callable(member)
                    if inspect.isfunction(target) and target.__doc__:
                        rows.append(
                            (f"{short}.{name}.{member_name}", target.__doc__)
                        )

    for table in ("_MESSAGES", "_REMEDIATION"):
        for code, text in getattr(findings_mod, table).items():
            rows.append((f"{table}[{code}]", text))

    spec = parse_system_topology_v1(
        {
            "version": "1",
            "profile_ref": "a",
            "objects": [
                {"kind": "api_service", "key": "a", "component_ref": "x"},
                {"kind": "process", "key": "p", "component_ref": "y"},
                {"kind": "runtime", "key": "rt", "runtime_ref": "r"},
                {"kind": "environment", "key": "e", "environment_ref": "env"},
                {"kind": "schedule", "key": "s"},
                {"kind": "deployment_unit", "key": "u"},
                {"kind": "external_queue", "key": "q", "resource_ref": "qr"},
            ],
            "relations": [
                {
                    "kind": "schedule_binding",
                    "key": "rs",
                    "schedule": "s",
                    "process": "p",
                    "runtime": "rt",
                },
                {
                    "kind": "deployment_binding",
                    "key": "rd",
                    "deployment_unit": "u",
                    "process": "p",
                    "environment": "e",
                },
                {
                    "kind": "queue_reference",
                    "key": "rq",
                    "process": "p",
                    "external_queue": "q",
                },
            ],
        }
    )
    prepared = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="a",
            dependency_corroboration=(
                DependencyCorroborationV1(
                    parent_component_ref="x",
                    child_component_ref="y",
                    child_component_type="process",
                ),
            ),
        )
    )
    # A SECOND context, because ``derive_unresolved_decisions`` has two arms and
    # the snapshot-free one reaches only three of its six strings. The three it
    # misses are precisely the absence notices that carried QA #250, #261 and
    # #262 — the surface most of this sequence's defects shipped on. A snapshot
    # that is same-account, truncated, partly unanswered and unobserved reaches
    # every one of them at once.
    from boomi_mcp.compiler.system_topology.context import (
        DiscoveryPageProvenanceV1,
        TopologyDiscoverySnapshotV1,
    )

    with_snapshot = prepare_topology_context(
        TopologyResolutionContextV1(
            profile="a",
            snapshot=TopologyDiscoverySnapshotV1(
                profile="a",
                captured_at="2026-01-01T00:00:00Z",
                source_revision="rev",
                service_release="rel",
                environment_inventory_observed=False,
                pagination=(
                    DiscoveryPageProvenanceV1(
                        component_type="process",
                        returned_count=1,
                        total_available=9,
                        has_more=True,
                    ),
                    DiscoveryPageProvenanceV1(
                        component_type="queue", returned_count=0, observed=False
                    ),
                ),
            ),
        )
    )
    for context in (prepared, with_snapshot):
        for guidance in derive_guidance(spec, context):
            rows.append((f"guidance[{guidance.subject}]", guidance.message))
        for decision in derive_unresolved_decisions(spec, context):
            rows.append((f"decision[{decision.subject}]", decision.question))
    return rows


def _strip_quoted_refutations(text):
    """Remove the exempt phrases, so what surrounds them is still read."""
    import re

    normalized = re.sub(r"\s+", " ", text).strip().lower()
    for phrase in _QUOTE_AND_REFUTE_PHRASES:
        normalized = normalized.replace(phrase, " ")
    return normalized


def test_no_published_surface_asserts_a_live_universal():
    """QA #264/#266/#267. Every surface a caller can read, in one census.

    Each earlier version read fewer surfaces than the last defect used, and the
    exemption was module-granular, which hid a genuine universal four list items
    from the citation it was granted for.
    """
    import re

    rows = _all_published_prose()

    # Per-surface minimums, so a surface disappearing is a FAILURE rather than a
    # smaller number. A single total was satisfied by the schema rows alone.
    kinds = {}
    for path, _ in rows:
        for prefix in ("schema", "module:", "_MESSAGES", "_REMEDIATION", "guidance[", "decision["):
            if path.startswith(prefix):
                kinds[prefix] = kinds.get(prefix, 0) + 1
                break
        else:
            kinds["docstring"] = kinds.get("docstring", 0) + 1
    # The prose surface is checked for EQUALITY against an independent AST
    # ledger, not against a hand-kept floor: the last two findings were both the
    # census missing a kind of definition (a package vs its children, a class vs
    # its methods), and a floor with slack in it cannot see a third.
    prose = sum(
        1
        for path, _ in rows
        if not path.startswith(
            ("schema", "_MESSAGES", "_REMEDIATION", "guidance[", "decision[")
        )
    )
    assert prose == _package_docstring_count(), (prose, _package_docstring_count())

    for prefix, minimum in (
        ("schema", 40),
        ("module:", 13),
        ("_MESSAGES", 14),
        ("_REMEDIATION", 14),
        ("guidance[", 4),
        # Six DISTINCT subjects, asserted separately below — a count alone was
        # satisfied exactly by the three the old single context could reach, so
        # the mechanism meant to make a vanished surface fail could not see that
        # half of one had never been there.
        ("decision[", 6),
    ):
        assert kinds.get(prefix, 0) >= minimum, (prefix, kinds.get(prefix, 0))

    # Every subject the derivers can EMIT must have been reached — derived from
    # their source, not from a hand-kept list. Pinning the reached set only
    # described what the two context arms happened to produce, so a subject
    # gated on something neither arm triggers was published uncensused. This is
    # the finding that keeps recurring (a rule applied at N-1 of N consumers),
    # and it is closed here for BOTH derived surfaces at once rather than for
    # the one that happened to fail last.
    for prefix, deriver, keyword in (
        ("guidance[", "derive_guidance", "message"),
        ("decision[", "derive_unresolved_decisions", "question"),
    ):
        reached = {
            _normalize_prose(text) for p, text in rows if p.startswith(prefix)
        }
        emitted = _emitted_strings(deriver, keyword)
        assert reached == emitted, (prefix, emitted - reached, reached - emitted)

    exempt = [(p, _strip_quoted_refutations(t)) for p, t in rows]
    assert _live_universals(exempt) == []

    # The refutations are still on the record, where they belong.
    joined = re.sub(r"\s+", " ", " ".join(t for _, t in rows)).lower()
    for phrase in _QUOTE_AND_REFUTE_PHRASES:
        assert phrase in joined, phrase


def test_the_published_schema_asserts_no_live_universal():
    """QA #255/#260. The surface six census rounds never rendered.

    Class docstrings ARE published schema — the lesson of QA #210 — and this
    schema publishes 60-odd of them verbatim. A retraction applied to
    ``derive_guidance`` alone therefore landed at one of four asserting sites,
    and the committed schema golden actively held the retracted sentence in
    place. A guard that reads the planner's output cannot see this by
    construction, so it is checked where it is published.
    """
    descriptions = _schema_descriptions()
    # Guard against the walk silently finding nothing, which would pass forever.
    assert len(descriptions) > 40, len(descriptions)
    assert _live_universals(descriptions) == []


def test_the_live_universal_guard_catches_what_was_retracted():
    """The control. Each string below was published by this schema and removed."""
    retracted = (
        (
            "/x",
            "Permanently gated-no-evidence in V1: the live account contains "
            "zero queue components.",
        ),
        (
            "/y",
            "Carries NO cron/interval body: every live schedule observed has "
            "an empty schedules: [] array, so schedule content has no evidence "
            "to model.",
        ),
        ("/z", "No API Service Component exists in either live profile today."),
        ("/w", "Schedule CONTENT has no evidence at all."),
        ("/v", "cron/interval shape has zero evidence."),
    )
    for row in retracted:
        assert _live_universals([row]), row[1]

    # ...and the capture-scoped replacements do not trip it, so the guard
    # discriminates between the claim's SCOPE rather than its subject matter.
    for row in (
        ("/a", "the capture behind this contract observed no queue components"),
        (
            "/b",
            "Every schedule body observed in the capture was an empty "
            "schedules: [] array, so cron/interval shape has no evidence to "
            "model from.",
        ),
        (
            "/c",
            "no API Service Component was observed when this contract's "
            "capability rows were captured",
        ),
    ):
        assert _live_universals([row]) == [], row[1]


_HASH_SEED_SCRIPT = """
import json, sys
sys.path.insert(0, {src!r})
from boomi_mcp.models.system_topology import (
    canonical_system_topology_json, canonical_system_topology_schema_json,
    parse_system_topology_v1,
)
payload = json.load(open({fixture!r}))
spec = parse_system_topology_v1(payload)
print('SPEC:' + canonical_system_topology_json(spec))
print('SCHEMA:' + canonical_system_topology_schema_json())
"""


@pytest.mark.parametrize("seed", ["0", "1", "12345"])
def test_canonical_output_is_identical_across_hash_seeds(seed):
    """Determinism ACROSS processes, which an in-process loop cannot show.

    ``PYTHONHASHSEED`` is read at interpreter startup, so within one process the
    seed is fixed and a repeated call proves only that the function is not
    randomized. Set iteration order — the realistic source of instability in a
    schema built from unions — only varies between processes.
    """
    code = _HASH_SEED_SCRIPT.format(
        src=_src, fixture=str(_FIXTURES / "system_topology_v1.json")
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src, PYTHONHASHSEED=seed),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    spec_line = [l for l in result.stdout.splitlines() if l.startswith("SPEC:")][0]
    schema_line = [l for l in result.stdout.splitlines() if l.startswith("SCHEMA:")][0]

    local = parse_system_topology_v1(_fixture("system_topology_v1.json"))
    assert spec_line[len("SPEC:") :] == canonical_system_topology_json(local)
    assert schema_line[len("SCHEMA:") :] == canonical_system_topology_schema_json()


def test_authored_list_order_is_preserved():
    """``sort_keys`` orders KEYS; list order is the document's meaning."""
    objects = [
        {"kind": "process", "key": "zeta", "component_ref": "$ref:z"},
        {"kind": "process", "key": "alpha", "component_ref": "$ref:a"},
    ]
    spec = parse_system_topology_v1(_doc(objects))
    produced = json.loads(canonical_system_topology_json(spec))
    assert [o["key"] for o in produced["objects"]] == ["zeta", "alpha"]


# ---------------------------------------------------------------------------
# Package exports
# ---------------------------------------------------------------------------


def test_models_package_exports_are_pinned():
    import boomi_mcp.models as models

    expected = {
        "SYSTEM_TOPOLOGY_VERSION",
        "TOPOLOGY_OBJECT_KINDS",
        "TOPOLOGY_RELATION_KINDS",
        "TOPOLOGY_RELATION_ROLES",
        "ApiServiceObjectV1",
        "ApiServiceRouteRelationV1",
        "DeploymentBindingRelationV1",
        "DeploymentUnitObjectV1",
        "DocumentCacheObjectV1",
        "DocumentCacheUseRelationV1",
        "EnvironmentObjectV1",
        "EventStreamReferenceRelationV1",
        "ExternalEventStreamObjectV1",
        "ExternalQueueObjectV1",
        "OpaquePlatformRefV1",
        "ProcessCallRelationV1",
        "ProcessObjectV1",
        "ProcessPropertyObjectV1",
        "ProcessPropertyUseRelationV1",
        "QueueReferenceRelationV1",
        "RuntimeObjectV1",
        "ScheduleBindingRelationV1",
        "ScheduleObjectV1",
        "SystemTopologyDiagnostic",
        "SystemTopologySpecV1",
        "SystemTopologyValidationError",
        "TopologyComponentRefV1",
        "TopologyObjectKeyV1",
        "TopologyObjectV1",
        "TopologyRelationKeyV1",
        "TopologyRelationV1",
        "canonical_system_topology_json",
        "canonical_system_topology_schema_json",
        "parse_system_topology_v1",
        "system_topology_v1_json_schema",
    }
    # EXACT, by set difference against the WHOLE ``__all__`` — not a predicate
    # filter over it. Filtering first meant an unexpected export that matched
    # none of the predicates (``SystemTopologyUnexpectedV1``) was invisible to
    # the equality assertion, so the surface could widen while the "exact" pin
    # stayed green. The pre-#144 names are enumerated instead, so anything that
    # is neither those nor these fails.
    # The pre-#144 surface, ENUMERATED. Prefix allowances ({"Process*", "Cache*"}
    # and friends) left a whole namespace open: an added ``ProcessTopologyUnexpectedV1``
    # matched a prefix and bypassed the check, so the "exact whole-surface pin"
    # was still a predicate. Derived once from the branch baseline
    # (9258ac7:src/boomi_mcp/models/__init__.py) and pinned here.
    _PRE_144_EXPORTS = {
        "BranchLegV1",
        "BranchNodeV1",
        "CacheGetNodeV1",
        "CachePutNodeV1",
        "CacheRemoveNodeV1",
        "CombineDocumentsOpV1",
        "ComponentRefV1",
        "ConnectorCallNodeV1",
        "CurrentPropertySourceV1",
        "CustomScriptingOpV1",
        "DataProcessNodeV1",
        "DataProcessOperationV1",
        "DdpPropertySourceV1",
        "DecisionFalseArmV1",
        "DecisionNodeV1",
        "DecisionOperandV1",
        "DecisionTrueArmV1",
        "DocumentCacheRetrieveNodeV1",
        "DppPropertySourceV1",
        "ErrorScopeV1",
        "ExceptionNodeV1",
        "FlowControlNodeV1",
        "IdempotencyContractRefV1",
        "IdempotencyEvidenceV1",
        "IntegrationComponentSpec",
        "IntegrationSpecV1",
        "KeyReferenceIdempotencyV1",
        "LinearNodeV1",
        "MapRefNodeV1",
        "MessageNodeV1",
        "PROCESS_IR_V1_CAPABILITIES",
        "PROCESS_IR_VERSION",
        "PipelineEdgeKind",
        "PipelineEdgeSpec",
        "PipelineSpec",
        "PipelineStageKind",
        "ProcessCallNodeV1",
        "ProcessIRDiagnostic",
        "ProcessIRV1",
        "ProcessIRValidationError",
        "ProcessNodeV1",
        "ProfilePropertySourceV1",
        "PropertySourceV1",
        "RetryPolicyV1",
        "ReturnDocumentsNodeV1",
        "SequenceNodeV1",
        "SetDdpNodeV1",
        "SetDppNodeV1",
        "SourceEndpointV1",
        "SplitDocumentsOpV1",
        "StageCardinality",
        "StageContextEffect",
        "StageFailureBehavior",
        "StageSideEffect",
        "StageSpec",
        "StaticOperandV1",
        "StaticPropertySourceV1",
        "StopNodeV1",
        "TargetEndpointV1",
        "TrackOperandV1",
        "TryCatchBodyStepV1",
        "TryCatchCatchBodyV1",
        "TryCatchNodeV1",
        "TryCatchTryBodyV1",
        "VerifiedActionIdempotencyV1",
        "canonical_process_ir_json",
        "canonical_process_ir_schema_json",
        "parse_process_ir_v1",
        "process_ir_v1_json_schema",
    }
    # The #145 (M12.10) recipe-contribution surface, ENUMERATED for exactly the
    # reason the pre-#144 set is: a prefix allowance like "Recipe*" would leave a
    # namespace open, and this pin exists to catch a widening surface, not to
    # describe one.
    _ISSUE_145_EXPORTS = {
        "AddTopologyObjectV1",
        "AddTopologyRelationV1",
        "AppendRootTerminalLegV1",
        "ComponentContributionV1",
        "ConstraintCheckV1",
        "ConstraintRequirementV1",
        "InsertRootLinearStepV1",
        "ProcessIRPatchOperationV1",
        "ProcessIRPatchV1",
        "RECIPE_COMPONENT_TYPES",
        "RECIPE_CONTRIBUTION_KINDS",
        "RECIPE_CONTRIBUTION_VERSION",
        "RECIPE_EXCLUDED_COMPONENT_TYPES",
        "RecipeComponentKey",
        "RecipeComponentType",
        "RecipeContributionV1",
        "RecipeContributionValidationError",
        "RecipeSemanticId",
        "RequireCapabilityV1",
        "RequireComponentV1",
        "RequireProcessV1",
        "RequireTopologyObjectV1",
        "RequireTopologyRelationV1",
        "SetProcessRootV1",
        "SystemTopologyPatchOperationV1",
        "SystemTopologyPatchV1",
        "canonical_recipe_contribution_json",
        "canonical_recipe_contribution_schema_json",
        "canonical_recipe_contributions_json",
        "parse_recipe_contribution",
        "recipe_contribution_v1_json_schema",
        "scan_forbidden_recipe_shape",
        "validate_contribution_object",
    }
    # The #146 (M12.11) MCP authoring surface, ENUMERATED for the same reason as
    # the two sets above: an "Authoring*" prefix allowance would leave a
    # namespace open, and this pin exists to catch a widening surface.
    _ISSUE_146_EXPORTS = {
        "AUTHORING_ACTIONS",
        "AUTHORING_CONTRACT_VERSION",
        "AUTHORING_INTENT_KINDS",
        "ArtifactFingerprintV1",
        "AuthoringBuildProvenanceV1",
        "AuthoringCompileResultV1",
        "AuthoringDiagnosticV1",
        "AuthoringPlanResultV1",
        "AuthoringRequestV1",
        "AuthoringRevisionBindingV1",
        "CapabilityGapV1",
        "ComponentDependencyEdgeV1",
        "DecisionResolutionV1",
        "IntegrationSpecAuthoringIntentV1",
        "LiveDeploymentComparisonV1",
        "ProcessCfgSummaryV1",
        "ProcessIRAuthoringIntentV1",
        "RecipeAuthoringIntentV1",
        "RecipeInvocationRequestV1",
        "RequiredDecisionV1",
        "ResolvedReferenceSummaryV1",
        "TopologyRelationSummaryV1",
        "ValidationReportSummaryV1",
        "authoring_build_provenance_v1_json_schema",
        "authoring_compile_result_v1_json_schema",
        "authoring_plan_result_v1_json_schema",
        "authoring_request_v1_json_schema",
        "authoring_revision_binding_v1_json_schema",
        "canonical_authoring_json",
        "sort_authoring_diagnostics",
    }
    non_topology = (
        set(models.__all__)
        - expected
        - _PRE_144_EXPORTS
        - _ISSUE_145_EXPORTS
        - _ISSUE_146_EXPORTS
    )
    assert non_topology == set(), non_topology
    assert expected <= set(models.__all__), expected - set(models.__all__)
    assert _ISSUE_145_EXPORTS <= set(models.__all__), _ISSUE_145_EXPORTS - set(
        models.__all__
    )
    assert _ISSUE_146_EXPORTS <= set(models.__all__), _ISSUE_146_EXPORTS - set(
        models.__all__
    )
    for name in expected | _ISSUE_145_EXPORTS | _ISSUE_146_EXPORTS:
        assert hasattr(models, name), name


def test_models_package_does_not_export_the_recipe_engine():
    """``boomi_mcp.models`` is the authored-CONTRACT surface.

    The four contribution models belong here; the registry, composer and engine
    do not — for the same reason #144 kept the topology PLANNER out. An execution
    engine reachable through an authoring namespace is one import away from
    appearing in an LLM-facing schema.
    """
    import boomi_mcp.models as models

    forbidden = {
        "RecipeRegistry",
        "run_recipes",
        "production_registry",
        "build_test_registry",
        "MaterializationCatalog",
        "RecipeRequestV1",
        "compose",
    }
    assert forbidden.isdisjoint(set(models.__all__))
    for name in forbidden:
        assert not hasattr(models, name), name


@pytest.mark.parametrize(
    "intruder",
    [
        "SystemTopologyUnexpectedV1",
        # The prefix-allowance escape: these matched ``Process*``/``Cache*`` and
        # slipped straight through the predicate version of the pin.
        "ProcessTopologyUnexpectedV1",
        "CacheUnexpectedV1",
        "PROCESS_IR_UNEXPECTED",
        "BranchUnexpectedV1",
        "CombineUnexpectedV1",
    ],
)
def test_the_export_pin_would_notice_an_unexpected_name(intruder):
    """Positive control, driving the REAL predicate the pin uses.

    Its first version filtered by prefix, so an export matching one bypassed the
    equality assertion entirely and the surface could widen while the "exact"
    pin stayed green. This exercises the same set difference the guard performs.
    """
    import boomi_mcp.models as models

    expected_topology = {
        name for name in models.__all__ if name.startswith("SYSTEM_TOPOLOGY_")
    }
    pre_144 = set(models.__all__) - expected_topology
    doctored = list(models.__all__) + [intruder]
    leftover = set(doctored) - expected_topology - pre_144
    assert leftover == {intruder}, (intruder, leftover)


def test_the_derived_reflection_tables_are_immutable():
    """They are validation authority, not a convenience view.

    The endpoint matrix, the semantic-duplicate key and the invariant checker
    all consume ``TOPOLOGY_RELATION_ROLES``, so a caller who mutated the public
    mapping could delete a role from a relation and turn an unresolved
    reference into a valid planned one. A plain dict handed that power to
    anyone who imported it.
    """
    from types import MappingProxyType

    from boomi_mcp.models.system_topology import TOPOLOGY_RELATION_ROLES

    assert isinstance(TOPOLOGY_RELATION_ROLES, MappingProxyType)
    with pytest.raises(TypeError):
        TOPOLOGY_RELATION_ROLES["schedule_binding"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        del TOPOLOGY_RELATION_ROLES["schedule_binding"]  # type: ignore[attr-defined]
    # The kind tuples are tuples, so their contents cannot be edited either.
    for roles in TOPOLOGY_RELATION_ROLES.values():
        assert isinstance(roles, tuple)


def test_the_planner_is_not_exported_through_models():
    """``boomi_mcp.models`` is the AUTHORED surface; the planner is derived."""
    import boomi_mcp.models as models

    for forbidden in (
        "plan_system_topology",
        "validate_system_topology",
        "SystemTopologyPlanV1",
        "TopologyResolutionContextV1",
    ):
        assert forbidden not in set(models.__all__)
        assert not hasattr(models, forbidden)


def test_spec_model_is_strict_at_the_root():
    assert SystemTopologySpecV1.model_config["extra"] == "forbid"



def test_the_doctype_screen_matches_the_tool_layers():
    """The XXE screen is a COPY; drift between the two is the failure mode.

    The secret list and the ``$ref`` rule each have a pinned-equality test. This
    one had behavioral tests but nothing tying it to the pattern it mirrors, so
    a tightening in ``schema_discovery`` could leave the topology copy behind.
    """
    from boomi_mcp.categories import schema_discovery
    from boomi_mcp.compiler.system_topology import evidence

    assert evidence._DOCTYPE_RE.pattern == schema_discovery._DOCTYPE_RE.pattern
    for hostile in ("<!DOCTYPE x>", "<!ENTITY y>", "<!  doctype z>", "<!dOcTyPe q>"):
        assert evidence._DOCTYPE_RE.search(hostile), hostile
        assert schema_discovery._DOCTYPE_RE.search(hostile), hostile
    assert not evidence._DOCTYPE_RE.search("<process/>")


def test_the_component_ref_rule_is_stricter_than_processirs_only_on_control_chars():
    """The copy is not byte-identical to ProcessIR's — and that is deliberate.

    The topology rule additionally rejects control characters, because a
    newline inside a key would break the one-finding-per-line shape every log
    consumer assumes. Pinned so the difference is a decision on the record
    rather than an accident, and so the two cannot diverge further unnoticed.
    """
    from boomi_mcp.models import process_ir
    from boomi_mcp.models import system_topology as st

    # Identical on every ordinary form.
    for value in ("$ref:key", "literal-id", "a", "a-b_c.d"):
        assert process_ir._validate_component_ref(value) == value
        assert st._validate_component_ref(value) == value
    for value in ("", " x", "x ", "$ref:", "$ref:a b"):
        with pytest.raises(Exception):
            process_ir._validate_component_ref(value)
        with pytest.raises(Exception):
            st._validate_component_ref(value)

    # And differ only here.
    control = "id\x01with-control"
    assert process_ir._validate_component_ref(control) == control
    with pytest.raises(Exception):
        st._validate_component_ref(control)
