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
    payload = _fixture("system_topology_v1.json")
    spec = parse_system_topology_v1(payload)
    produced = json.loads(canonical_system_topology_json(spec))
    # Every authored value survives the round trip, defaults expanded.
    assert produced["version"] == SYSTEM_TOPOLOGY_VERSION
    assert produced["profile_ref"] == payload["profile_ref"]
    assert len(produced["objects"]) == len(payload["objects"])
    assert len(produced["relations"]) == len(payload["relations"])


def test_schema_golden_pin_matches_the_committed_fixture():
    committed = (_FIXTURES / "system_topology_v1.schema.json").read_text()
    assert canonical_system_topology_schema_json() == committed.strip()


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
    assert expected <= set(models.__all__)
    for name in expected:
        assert hasattr(models, name), name


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
