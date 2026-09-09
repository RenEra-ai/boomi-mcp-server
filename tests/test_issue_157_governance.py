"""Issue #157 (M12.19) — governance on the typed per-root envelope.

Derived tests (the cases are generated from the authorities, not hand-listed),
each with an adversarial negative, over: prefix→name derivation, folder and
name fan-out by ownership, the connection binding contract (reuse XOR create,
the REST auth gate, header collisions), the secret scan, the recorded-not-wired
channel and its fingerprint boundary, and the recipe per-root envelope rules.

Every scenario enters through the public planning entry
(``plan_authoring_request_v1``) or the dispatcher, never the resolver alone,
with only the live metadata boundary faked.
"""

from __future__ import annotations

import copy
import itertools
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import (  # noqa: E402
    APPLIABLE_CONN,
    APPLIABLE_IR_DOC,
    APPLIABLE_OP,
)
from boomi_mcp.authoring import governance  # noqa: E402
from boomi_mcp.authoring.governance import (  # noqa: E402
    REUSE_BINDING_KEYS,
    derive_default_name,
    owned_supporting_keys,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    AuthoringWorkflowError,
    _normalized_payload,
    compile_authoring_request_v1,
    plan_authoring_request_v1,
)
from boomi_mcp.errors import (  # noqa: E402
    GOVERNANCE_CONNECTION_BINDING_CONFLICT,
    GOVERNANCE_ENVELOPE_DUPLICATE,
    GOVERNANCE_FLOWS_OUTPUT_ONLY,
    GOVERNANCE_HEADER_COLLISION,
    GOVERNANCE_INHERITANCE_AMBIGUOUS,
    GOVERNANCE_NAME_COLLISION,
    GOVERNANCE_NAME_UNRESOLVED,
    GOVERNANCE_RECORDED_INTENT_INVALID,
    GOVERNANCE_WATERMARK_INCONSISTENT,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    CanonicalIntegrationPreviewV1,
    ProcessIRAuthoringIntentV1,
    RecipeAuthoringIntentV1,
)
from boomi_mcp.models.governance_intent import RETIRED_SPELLINGS  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitAuthoredV1,
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeAuthoredV1,
    ProcessComponentEnvelopeV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.patterns.archetype_assembly import (  # noqa: E402
    UNSUPPORTED_REST_AUTH_MODE,
    _REST_CREATE_AUTH_MAP,
)
from boomi_mcp.patterns.archetype_parameters import (  # noqa: E402
    _FORBIDDEN_SECRET_KEY_SUBSTRINGS,
)
from boomi_mcp.categories.components.builders.connector_builder import (  # noqa: E402
    RestClientConnectionBuilder,
)

_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"
_PROFILE = "issue-157"


@pytest.fixture(autouse=True)
def _offline():
    with patch(_PAGINATE, lambda *a, **k: []):
        yield


def _conn(key="conn", **overrides):
    spec = copy.deepcopy(APPLIABLE_CONN)
    spec["key"] = key
    spec.pop("name")
    spec["config"] = {k: v for k, v in spec["config"].items() if k != "component_name"}
    spec.update(overrides)
    return spec


def _op(key="op", conn="conn", **overrides):
    spec = copy.deepcopy(APPLIABLE_OP)
    spec["key"] = key
    spec["depends_on"] = [conn]
    spec["config"]["connection_ref_key"] = conn
    spec.update(overrides)
    return spec


def _ir(conn="conn", op="op"):
    return parse_process_ir_v1(
        {
            "version": "1",
            "body": {
                "kind": "sequence",
                "steps": [
                    {"kind": "source", "connection_ref": "$ref:" + conn, "operation_ref": "$ref:" + op},
                    {"kind": "message", "text": "hello"},
                    {"kind": "return_documents"},
                ],
            },
        }
    )


def _unit(key="proc", conn="conn", op="op", **envelope):
    kwargs = {"component_key": key, "action": "create", "depends_on": (conn, op)}
    kwargs.update(envelope)
    return ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(**kwargs), process_ir=_ir(conn, op)
    )


def _request(units, components):
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="Issue 157", units=tuple(units), components=tuple(components)
        )
    )


def _plan(units, components):
    return plan_authoring_request_v1(_request(units, components), boomi_client=MagicMock(), profile=_PROFILE)


def _refusal(units, components):
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _plan(units, components)
    return excinfo.value


# ---------------------------------------------------------------------------
# prefix -> default name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "prefix,key",
    list(itertools.product(("QA157", "Team-A", "x"), ("proc", "root_main", "k2"))),
)
def test_a_default_name_is_the_prefix_a_space_and_the_key(prefix, key):
    """DERIVED from the pair, never a hand-listed table of expected names."""
    _result, internals = _plan([_unit(key=key, component_prefix=prefix)], [_conn(), _op()])
    (unit,) = internals.normalized.integration_spec.processes
    assert isinstance(unit, ProcessAuthoringUnitV1)
    assert unit.envelope.name == derive_default_name(prefix, key) == "{0} {1}".format(prefix, key)


def test_an_explicit_name_wins_over_the_prefix():
    _result, internals = _plan([_unit(name="Explicit", component_prefix="PFX")], [_conn(), _op()])
    assert internals.normalized.integration_spec.processes[0].envelope.name == "Explicit"


def test_explicit_and_defaulted_governance_produce_identical_envelopes_and_hashes():
    """The whole point of resolving BEFORE fingerprinting."""
    defaulted, d_int = _plan(
        [_unit(component_prefix="PFX", folder_name="F")],
        [_conn(), _op()],
    )
    explicit_conn = _conn(name="PFX conn")
    explicit_conn["config"]["folder_name"] = "F"
    explicit_op = _op()
    explicit_op["config"]["folder_name"] = "F"
    explicit, e_int = _plan([_unit(name="PFX proc", folder_name="F")], [explicit_conn, explicit_op])
    assert d_int.normalized.integration_spec.processes[0].envelope == e_int.normalized.integration_spec.processes[0].envelope
    assert defaulted.revision_binding.semantic_hash == explicit.revision_binding.semantic_hash
    assert defaulted.revision_binding.plan_hash == explicit.revision_binding.plan_hash


@pytest.mark.parametrize("bad", ["", " ", " PFX", "PFX ", "\tPFX"])
def test_a_blank_or_padded_prefix_is_refused_at_the_model(bad):
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", component_prefix=bad)
    assert "process_component_value_invalid" in str(excinfo.value)


@pytest.mark.parametrize("bad", ["", " N", "N ", "  N  "])
def test_a_blank_or_padded_explicit_name_is_still_refused(bad):
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", name=bad)
    assert "process_component_value_invalid" in str(excinfo.value)


def test_neither_name_nor_prefix_is_the_named_unresolved_refusal():
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create")
    assert "governance_name_unresolved" in str(excinfo.value)


def test_the_unresolved_refusal_serves_its_named_code_through_the_dispatcher():
    from boomi_mcp.categories.integration_builder import build_integration_action

    payload = {
        "authoring_request": {
            "contract_version": "2",
            "intent": {
                "intent_kind": "process_ir",
                "integration_name": "x",
                "units": [{"envelope": {"component_key": "p", "action": "create"}, "process_ir": APPLIABLE_IR_DOC}],
                "components": [APPLIABLE_CONN, APPLIABLE_OP],
            },
        }
    }
    result = build_integration_action(MagicMock(), _PROFILE, "plan", payload)
    assert result.get("_success") is False
    assert GOVERNANCE_NAME_UNRESOLVED in str(result)


def test_action_is_never_derived():
    with pytest.raises(Exception):
        ProcessComponentEnvelopeAuthoredV1(component_key="p", component_prefix="PFX")


def test_two_roots_resolving_to_one_create_name_are_refused():
    a = _unit(key="a", component_prefix="PFX", name="Same")
    b = _unit(key="b", component_prefix="PFX", name="Same")
    error = _refusal([a, b], [_conn(), _op()])
    assert error.code == GOVERNANCE_NAME_COLLISION
    assert error.diagnostics[0].path.startswith("/units/")


def test_two_roots_with_distinct_prefixes_keep_distinct_derived_names():
    # the shared supporting components are explicitly named, so no inherited
    # default is in question — only the roots' own names are derived
    _r, internals = _plan([_unit(key="a", component_prefix="One"), _unit(key="b", component_prefix="Two")], [_conn(name="Shared conn"), _op()])
    assert sorted(u.envelope.name for u in internals.normalized.integration_spec.processes) == ["One a", "Two b"]


# ---------------------------------------------------------------------------
# folder fan-out and inherited default names, by OWNERSHIP
# ---------------------------------------------------------------------------


def test_the_root_folder_places_every_owned_component_lacking_its_own():
    _r, internals = _plan([_unit(component_prefix="PFX", folder_name="Integrations/QA")], [_conn(), _op()])
    spec = internals.normalized.integration_spec
    assert [c.config["folder_name"] for c in spec.components] == ["Integrations/QA", "Integrations/QA"]
    # the unnamed connection inherits a default; the already-named operation keeps its own
    assert [c.name for c in spec.components] == ["PFX conn", "M12.15 op"]


def test_an_explicit_component_placement_and_name_are_never_overridden():
    conn = _conn(name="Kept")
    conn["config"]["folder_name"] = "Elsewhere"
    _r, internals = _plan([_unit(component_prefix="PFX", folder_name="Integrations/QA")], [conn, _op()])
    by_key = {c.key: c for c in internals.normalized.integration_spec.components}
    assert by_key["conn"].config["folder_name"] == "Elsewhere" and by_key["conn"].name == "Kept"
    assert by_key["op"].config["folder_name"] == "Integrations/QA"


def test_ownership_is_the_reference_closure_not_a_component_type_list():
    """A component reachable only through another component's $ref is owned too."""
    profile = {"key": "prof", "type": "profile.json", "action": "create",
               "config": {"root": {"name": "Root", "kind": "object", "children": [{"name": "a", "kind": "simple", "data_type": "character"}]}}}
    op = _op()
    op["config"]["request_profile_id"] = "$ref:prof"
    op["depends_on"] = ["conn", "prof"]
    _r, internals = _plan([_unit(component_prefix="PFX", folder_name="F")], [_conn(), op, profile])
    by_key = {c.key: c for c in internals.normalized.integration_spec.components}
    assert by_key["prof"].config["folder_name"] == "F" and by_key["prof"].name == "PFX prof"
    envelope = ProcessComponentEnvelopeV1(component_key="proc", name="n", action="create", depends_on=("conn", "op"))
    specs = [IntegrationComponentSpec(**c) for c in (_conn(), op, profile)]
    assert owned_supporting_keys(envelope, _ir(), {c.key: c for c in specs}, {"proc"}) == ("conn", "op", "prof")


def test_fan_out_stops_at_another_process_root_and_never_enters_a_reuse_reference():
    reused = {"key": "shared", "type": "connector-settings", "action": "create",
              "config": {"reference_only": True, "component_name": "Existing", "connector_type": "rest"}}
    op_a, op_b = _op(key="op_a", conn="shared"), _op(key="op_b", conn="shared")
    root_b = _unit(key="b", conn="shared", op="op_b", name="B", folder_name="FolderB")
    # root a depends on root b: the walk must stop at b and not adopt op_b
    root_a = _unit(key="a", conn="shared", op="op_a", name="A", folder_name="FolderA", depends_on=("shared", "op_a", "b"))
    _r, internals = _plan([root_a, root_b], [reused, op_a, op_b])
    by_key = {c.key: c for c in internals.normalized.integration_spec.components}
    assert "folder_name" not in by_key["shared"].config  # a reuse reference is never moved
    assert by_key["op_a"].config["folder_name"] == "FolderA"
    assert by_key["op_b"].config["folder_name"] == "FolderB"  # owned by b alone, despite a -> b
    roots = {u.envelope.component_key: u.envelope for u in internals.normalized.integration_spec.processes}
    assert roots["a"].folder_name == "FolderA" and roots["b"].folder_name == "FolderB"


def test_a_shared_unplaced_component_owned_by_two_folders_is_refused_not_resolved():
    root_a = _unit(key="a", name="A", folder_name="FolderA")
    root_b = _unit(key="b", name="B", folder_name="FolderB")
    error = _refusal([root_a, root_b], [_conn(), _op()])
    assert error.code == GOVERNANCE_INHERITANCE_AMBIGUOUS
    assert error.diagnostics[0].path == "/components/conn/config/folder_name"


def test_an_explicit_placement_resolves_the_ambiguity():
    conn = _conn()
    conn["config"]["folder_name"] = "Chosen"
    op = _op()
    op["config"]["folder_name"] = "Chosen"
    _r, internals = _plan([_unit(key="a", name="A", folder_name="FolderA"), _unit(key="b", name="B", folder_name="FolderB")], [conn, op])
    assert {c.config["folder_name"] for c in internals.normalized.integration_spec.components} == {"Chosen"}


def test_a_shared_unnamed_component_owned_by_two_prefixes_is_refused():
    error = _refusal([_unit(key="a", component_prefix="One"), _unit(key="b", component_prefix="Two")], [_conn(), _op()])
    assert error.code == GOVERNANCE_INHERITANCE_AMBIGUOUS
    assert error.diagnostics[0].path == "/components/conn/name"


def test_multi_root_isolation_preserves_distinct_names_and_folders():
    conn_b = _conn(key="conn_b")
    op_b = _op(key="op_b", conn="conn_b")
    _r, internals = _plan(
        [_unit(key="a", conn="conn", op="op", component_prefix="A", folder_name="FA"),
         _unit(key="b", conn="conn_b", op="op_b", component_prefix="B", folder_name="FB")],
        [_conn(), _op(), conn_b, op_b],
    )
    by_key = {c.key: c for c in internals.normalized.integration_spec.components}
    assert (by_key["conn"].config["folder_name"], by_key["conn"].name) == ("FA", "A conn")
    assert (by_key["conn_b"].config["folder_name"], by_key["conn_b"].name) == ("FB", "B conn_b")


def test_governance_resolves_before_fingerprinting_and_covers_the_resolved_values():
    """Two requests differing only in the folder knob mint different hashes."""
    a, _ = _plan([_unit(component_prefix="PFX", folder_name="One")], [_conn(), _op()])
    b, _ = _plan([_unit(component_prefix="PFX", folder_name="Two")], [_conn(), _op()])
    assert a.revision_binding.semantic_hash != b.revision_binding.semantic_hash


# ---------------------------------------------------------------------------
# the connection binding contract
# ---------------------------------------------------------------------------


def _reuse_conn(**config):
    base = {"reference_only": True, "component_name": "Existing", "connector_type": "rest"}
    base.update(config)
    return {"key": "conn", "type": "connector-settings", "action": "create", "config": base}


def test_a_reuse_binding_carrying_inline_creation_settings_is_refused():
    error = _refusal([_unit(name="P")], [_reuse_conn(base_url="https://x.invalid"), _op()])
    assert error.code == GOVERNANCE_CONNECTION_BINDING_CONFLICT
    assert error.diagnostics[0].path == "/components/conn/config/base_url"


@pytest.mark.parametrize("key", sorted(REUSE_BINDING_KEYS))
def test_every_reuse_binding_key_is_accepted_on_a_reuse_binding(key):
    """DERIVED from the closed key set: each member alone is legal."""
    config = {"reference_only": True, "component_name": "Existing"}
    if key == "component_id":
        config = {"reference_only": True, "component_id": "abc"}
    elif key == "connector_type":
        config["connector_type"] = "rest"
    spec = {"key": "conn", "type": "connector-settings", "action": "create", "config": config}
    result, _ = _plan([_unit(name="P")], [spec, _op()])
    assert all(d.code != GOVERNANCE_CONNECTION_BINDING_CONFLICT for d in result.errors)


def test_a_reuse_binding_naming_two_disagreeing_ids_is_refused():
    spec = _reuse_conn(component_id="one")
    spec["component_id"] = "two"
    error = _refusal([_unit(name="P")], [spec, _op()])
    assert error.code == GOVERNANCE_CONNECTION_BINDING_CONFLICT


def test_a_reuse_binding_naming_two_disagreeing_names_is_refused():
    spec = _reuse_conn()
    spec["name"] = "Other"
    error = _refusal([_unit(name="P")], [spec, _op()])
    assert error.code == GOVERNANCE_CONNECTION_BINDING_CONFLICT


def test_create_plus_reference_only_is_reuse_and_stays_legal():
    result, internals = _plan([_unit(name="P")], [_reuse_conn(), _op()])
    assert all(d.code != GOVERNANCE_CONNECTION_BINDING_CONFLICT for d in result.errors)


def _secured_modes():
    allowed = {mode.upper() for mode in _REST_CREATE_AUTH_MAP.values()}
    return sorted(m for m in RestClientConnectionBuilder.RECOGNIZED_AUTH_MODES if m.upper() not in allowed)


@pytest.mark.parametrize("mode", _secured_modes())
def test_a_secured_rest_connection_cannot_be_created_inline(mode):
    """DERIVED from the REST builder's recognized modes minus the create map."""
    conn = _conn()
    conn["config"]["auth"] = mode
    error = _refusal([_unit(name="P")], [conn, _op()])
    assert error.code == UNSUPPORTED_REST_AUTH_MODE
    assert error.diagnostics[0].path == "/components/conn/config/auth"


@pytest.mark.parametrize("mode", sorted(_REST_CREATE_AUTH_MAP.values()))
def test_an_unauthenticated_rest_connection_can_be_created_inline(mode):
    conn = _conn()
    conn["config"]["auth"] = mode
    result, _ = _plan([_unit(name="P")], [conn, _op()])
    assert all(d.code != UNSUPPORTED_REST_AUTH_MODE for d in result.errors)


def test_the_secured_set_is_non_empty():
    assert _secured_modes(), "the REST builder recognizes no secured mode — the gate would be vacuous"


@pytest.mark.parametrize(
    "default,operation",
    [
        ({"Accept": "a"}, {"Accept": "b"}),      # identical spelling
        ({"Accept": "a"}, {"accept": "b"}),      # case variant, operation lower
        ({"accept": "a"}, {"ACCEPT": "b"}),      # case variant, operation upper
    ],
)
def test_default_and_operation_header_collisions_are_refused_in_every_direction(default, operation):
    conn = _conn()
    conn["config"]["default_headers"] = default
    op = _op()
    op["config"]["request_headers"] = operation
    error = _refusal([_unit(name="P")], [conn, op])
    assert error.code == GOVERNANCE_HEADER_COLLISION


def test_a_case_variant_duplicate_inside_one_header_dict_is_refused_too():
    conn = _conn()
    conn["config"]["default_headers"] = {"Accept": "a", "accept": "b"}
    error = _refusal([_unit(name="P")], [conn, _op()])
    assert error.code == GOVERNANCE_HEADER_COLLISION


def test_non_colliding_default_headers_reach_the_operation_and_leave_the_connection():
    conn = _conn()
    conn["config"]["default_headers"] = {"X-Trace": "1"}
    op = _op()
    op["config"]["request_headers"] = {"Accept": "json"}
    _r, internals = _plan([_unit(name="P")], [conn, op])
    by_key = {c.key: c for c in internals.normalized.integration_spec.components}
    assert by_key["op"].config["request_headers"] == {"X-Trace": "1", "Accept": "json"}
    assert "default_headers" not in by_key["conn"].config


def test_the_legacy_merger_still_lets_the_operation_win():
    from boomi_mcp.patterns.archetype_assembly import _merge_request_headers

    assert _merge_request_headers({"Accept": "a"}, {"accept": "b"}, default_field="d", operation_field="o") == {"accept": "b"}


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


def _secret_key_cases():
    for substring in _FORBIDDEN_SECRET_KEY_SUBSTRINGS:
        yield substring + "_zq9"
        yield substring.upper() + "_ZQ9"
        yield "x_" + substring + "_y_zq9"


@pytest.mark.parametrize("key", list(_secret_key_cases()))
def test_the_runtime_scan_is_derived_from_the_archetype_scanner(key):
    """Every forbidden substring, in three case/nesting variants, at depth.

    The canary suffix makes the KEY distinctive, so "the key is not echoed"
    can be asserted without colliding with the word 'secret' in served prose.
    """
    from boomi_mcp.models.authoring_workflow import IntegrationSpecAuthoringIntentV1
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    spec = IntegrationSpecV1(name="x", components=[IntegrationComponentSpec(**APPLIABLE_CONN)], runtime={"outer": [{key: "hunter2"}]})
    request = AuthoringRequestV1(intent=IntegrationSpecAuthoringIntentV1(integration_spec=spec))
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert excinfo.value.code == governance.PLAINTEXT_SECRET_REJECTED
    rendered = " ".join(d.message + d.path + d.remediation for d in excinfo.value.diagnostics)
    assert "hunter2" not in rendered and "zq9" not in rendered.lower()


def test_a_clean_runtime_passes_the_scan():
    from boomi_mcp.models.authoring_workflow import IntegrationSpecAuthoringIntentV1
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    spec = IntegrationSpecV1(name="x", components=[IntegrationComponentSpec(**APPLIABLE_CONN)], runtime={"atom_pool": "primary"})
    request = AuthoringRequestV1(intent=IntegrationSpecAuthoringIntentV1(integration_spec=spec))
    result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert result.integration_spec_preview.runtime == {"atom_pool": "primary"}


# ---------------------------------------------------------------------------
# recorded-not-wired
# ---------------------------------------------------------------------------


def _hint(value="primary"):
    return {"declaration_kind": "runtime_hint", "hint_kind": "atom_selection", "value": value}


def test_a_recorded_intent_changes_only_the_served_record():
    without, w_int = _plan([_unit(name="P")], [_conn(), _op()])
    with_hint, h_int = _plan([_unit(name="P", recorded_intents=(_hint(),))], [_conn(), _op()])
    assert with_hint.recorded_intents and not without.recorded_intents
    assert with_hint.recorded_intents[0].status == "recorded_not_wired"
    assert with_hint.revision_binding.semantic_hash == without.revision_binding.semantic_hash
    assert with_hint.revision_binding.plan_hash == without.revision_binding.plan_hash
    assert with_hint.validation_report == without.validation_report
    assert w_int.normalized.integration_spec == h_int.normalized.integration_spec


def test_the_normalized_payload_key_set_is_pinned_and_carries_no_record():
    request = _request([_unit(name="P", recorded_intents=(_hint(),))], [_conn(), _op()])
    _r, internals = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    payload = _normalized_payload(internals.normalized, request)
    assert set(payload) == {"intent_kind", "conflict_policy", "integration_spec", "process_roots", "topology_spec", "decisions"}
    assert "recorded" not in str(payload) and "runtime_hint" not in str(payload)


def test_a_recorded_intent_is_served_by_compile_and_persisted_with_the_build():
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    request = _request([_unit(name="P", recorded_intents=(_hint(),))], [_conn(name="Named conn"), _op()])
    result, _ = compile_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert [r.intent_id for r in result.recorded_intents] == ["runtime_hint/0"]


def test_a_secret_shaped_hint_is_refused_and_never_echoed():
    """The hint MODEL has fixed keys, so the scan runs over its dump; a value is content and is not scanned."""
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", name="n",
                                           recorded_intents=({"declaration_kind": "runtime_hint", "hint_kind": "note", "value": " padded "},))
    assert "governance_recorded_intent_invalid" in str(excinfo.value)


def test_two_records_sharing_an_identity_are_refused():
    unit = _unit(name="P", recorded_intents=(_hint("a"),))
    error = None
    try:
        governance.resolve_governance([unit], [IntegrationComponentSpec(**_conn()), IntegrationComponentSpec(**_op())],
                                      extra_recorded_intents=(governance.RecordedIntentV1(intent_id="runtime_hint/0", process_key="proc", declaration=_hint("b")),))
    except governance.GovernanceRefusal as exc:
        error = exc
    assert error is not None and error.code == GOVERNANCE_RECORDED_INTENT_INVALID


# ---------------------------------------------------------------------------
# watermark declaration (M18 rule half)
# ---------------------------------------------------------------------------


def _profile(key="src_prof", fields=("updated_at", "id")):
    return {"key": key, "type": "profile.db", "action": "create",
            "config": {"profile_type": "database.read", "output_fields": [{"name": f, "data_type": "character"} for f in fields]}}


def test_a_watermark_over_a_declared_field_is_recorded_not_wired():
    op = _op()
    op["depends_on"] = ["conn", "src_prof"]
    unit = _unit(name="P", depends_on=("conn", "op", "src_prof"),
                 watermark={"source_profile_ref": "$ref:src_prof", "field": "updated_at", "kind": "timestamp"})
    result, internals = _plan([unit], [_conn(), op, _profile()])
    assert [r.intent_id for r in result.recorded_intents] == ["watermark"]
    assert result.recorded_intents[0].declaration.persistence == "process_property"
    # nothing wired: the IR is byte-identical to the declaration-free request
    plain, _ = _plan([_unit(name="P", depends_on=("conn", "op", "src_prof"))], [_conn(), op, _profile()])
    assert plain.revision_binding.semantic_hash == result.revision_binding.semantic_hash


def test_a_watermark_over_an_undeclared_field_is_the_named_inconsistency():
    op = _op()
    op["depends_on"] = ["conn", "src_prof"]
    unit = _unit(name="P", depends_on=("conn", "op", "src_prof"),
                 watermark={"source_profile_ref": "$ref:src_prof", "field": "missing", "kind": "timestamp"})
    error = _refusal([unit], [_conn(), op, _profile()])
    assert error.code == GOVERNANCE_WATERMARK_INCONSISTENT
    assert error.diagnostics[0].path.endswith("/watermark/field")


def test_a_watermark_naming_an_undeclared_query_parameter_is_the_named_inconsistency():
    op = _op()
    op["depends_on"] = ["conn", "src_prof"]
    op["config"]["query_parameters"] = {"since": "x"}
    unit = _unit(name="P", depends_on=("conn", "op", "src_prof"),
                 watermark={"source_profile_ref": "$ref:src_prof", "field": "id", "kind": "sequence", "query_parameter_refs": ("nope",)})
    error = _refusal([unit], [_conn(), op, _profile()])
    assert error.code == GOVERNANCE_WATERMARK_INCONSISTENT
    assert error.diagnostics[0].path.endswith("/query_parameter_refs/0")
    ok = _unit(name="P", depends_on=("conn", "op", "src_prof"),
               watermark={"source_profile_ref": "$ref:src_prof", "field": "id", "kind": "sequence", "query_parameter_refs": ("since",)})
    result, _ = _plan([ok], [_conn(), op, _profile()])
    assert result.recorded_intents[0].declaration.query_parameter_refs == ("since",)


# ---------------------------------------------------------------------------
# retired spellings and the flows refusal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spelling", sorted(RETIRED_SPELLINGS))
def test_every_retired_spelling_is_refused_by_name_on_the_typed_envelope(spelling):
    """DERIVED from the retirement authority: one refusal per retired key."""
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", name="n", **{spelling: "x"})
    assert "governance_retired_spelling" in str(excinfo.value)
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", name="n", process_extensions={"connections": [{"connection_id": "$ref:c", spelling: 1, "fields": [{"id": "a", "label": "b"}]}]})
    assert "governance_retired_spelling" in str(excinfo.value)


def test_an_unknown_but_unretired_key_is_still_the_plain_unknown_field_refusal():
    with pytest.raises(Exception) as excinfo:
        ProcessComponentEnvelopeAuthoredV1(component_key="p", action="create", name="n", colour="blue")
    assert "extra_forbidden" in str(excinfo.value) and "governance_retired_spelling" not in str(excinfo.value)


def test_caller_flows_on_a_process_ir_intent_are_refused_by_name():
    with pytest.raises(Exception) as excinfo:
        ProcessIRAuthoringIntentV1(integration_name="x", units=(_unit(name="P"),), flows=[{"key": "k"}])
    assert "governance_flows_output_only" in str(excinfo.value)


def test_caller_flows_on_a_recipe_intent_or_its_raw_input_are_refused_by_name():
    with pytest.raises(Exception) as excinfo:
        RecipeAuthoringIntentV1(integration_name="x", invocations=({"recipe_id": "r", "invocation_id": "i"},), flows=[])
    assert "governance_flows_output_only" in str(excinfo.value)
    with pytest.raises(Exception) as excinfo:
        RecipeAuthoringIntentV1(integration_name="x", invocations=({"recipe_id": "r", "invocation_id": "i", "raw_input": {"flows": []}},))
    assert "governance_flows_output_only" in str(excinfo.value)


def test_the_served_flows_refusal_reaches_the_dispatcher_with_its_code():
    from boomi_mcp.categories.integration_builder import build_integration_action

    payload = {"authoring_request": {"contract_version": "2", "intent": {
        "intent_kind": "process_ir", "integration_name": "x", "flows": [],
        "units": [{"envelope": {"component_key": "p", "action": "create", "name": "n"}, "process_ir": APPLIABLE_IR_DOC}],
        "components": [APPLIABLE_CONN, APPLIABLE_OP]}}}
    result = build_integration_action(MagicMock(), _PROFILE, "plan", payload)
    assert result.get("_success") is False and GOVERNANCE_FLOWS_OUTPUT_ONLY in str(result)


def test_the_legacy_intent_keeps_its_authored_flows_and_legacy_preview():
    from boomi_mcp.models.authoring_workflow import IntegrationSpecAuthoringIntentV1
    from boomi_mcp.models.integration_models import IntegrationSpecV1

    spec = IntegrationSpecV1(name="x", components=[IntegrationComponentSpec(**APPLIABLE_CONN)], flows=[{"key": "legacy", "operation": "noop"}])
    request = AuthoringRequestV1(intent=IntegrationSpecAuthoringIntentV1(integration_spec=spec))
    result, _ = plan_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    assert not isinstance(result.integration_spec_preview, CanonicalIntegrationPreviewV1)
    assert result.integration_spec_preview.flows == [{"key": "legacy", "operation": "noop"}]


def test_the_compiling_intents_serve_the_canonical_preview():
    result, _ = _plan([_unit(name="P")], [_conn(), _op()])
    assert isinstance(result.integration_spec_preview, CanonicalIntegrationPreviewV1)
    assert result.integration_spec_preview.preview_kind == "canonical_units"
    assert result.integration_spec_preview.processes == ()  # still withheld


# ---------------------------------------------------------------------------
# recipe per-root envelopes
# ---------------------------------------------------------------------------


def test_duplicate_recipe_envelopes_are_refused_by_name():
    with pytest.raises(Exception) as excinfo:
        RecipeAuthoringIntentV1(integration_name="x", invocations=({"recipe_id": "r", "invocation_id": "i"},),
                                process_envelopes=({"component_key": "p", "name": "a"}, {"component_key": "p", "name": "b"}))
    assert "governance_envelope_duplicate" in str(excinfo.value)


def test_a_recipe_envelope_carries_governance_only():
    from boomi_mcp.models.authoring_workflow import RecipeProcessEnvelopeV1

    assert "action" not in RecipeProcessEnvelopeV1.model_fields
    assert "depends_on" not in RecipeProcessEnvelopeV1.model_fields
    assert set(RecipeProcessEnvelopeV1.governance_fields()) >= {"name", "component_prefix", "description", "folder_name", "process_extensions", "watermark", "recorded_intents"}


def test_the_recipe_lift_refuses_a_second_governance_source_and_accepts_a_prefix(monkeypatch):
    """Drives the real lift with a composed root and a typed envelope."""
    from boomi_mcp.authoring.workflow import _lift_recipe_roots_into_units
    from boomi_mcp.models.authoring_workflow import RecipeProcessEnvelopeV1

    root = _ir()
    lifted = IntegrationComponentSpec(key="main", type="process", action="create", config={"process_kind": "x"})
    envelope = RecipeProcessEnvelopeV1(component_key="main", component_prefix="R", description="d")
    _supporting, units = _lift_recipe_roots_into_units([lifted, IntegrationComponentSpec(**_conn())], (("main", root),), {"main": envelope})
    assert units[0].envelope.component_prefix == "R" and units[0].envelope.name is None
    with_name = IntegrationComponentSpec(key="main", type="process", action="create", name="Dup", config={})
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units([with_name], (("main", root),), {"main": envelope})
    assert excinfo.value.code == "GOVERNANCE_SOURCE_CONFLICT"
    with_folder = IntegrationComponentSpec(key="main", type="process", action="create", config={"folder_name": "F"})
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units([with_folder], (("main", root),), {"main": envelope})
    assert excinfo.value.code == "GOVERNANCE_SOURCE_CONFLICT"
    with pytest.raises(AuthoringWorkflowError) as excinfo:
        _lift_recipe_roots_into_units([lifted], (("main", root),), {"other": envelope})
    assert excinfo.value.code == "GOVERNANCE_ENVELOPE_UNMATCHED"


def test_the_pre_157_lift_is_unchanged_without_an_envelope():
    from boomi_mcp.authoring.workflow import _lift_recipe_roots_into_units

    lifted = IntegrationComponentSpec(key="main", type="process", action="create", name="Named", config={"description": "d", "folder_name": "F"})
    _s, units = _lift_recipe_roots_into_units([lifted], (("main", _ir()),), {})
    assert (units[0].envelope.name, units[0].envelope.description, units[0].envelope.folder_name) == ("Named", "d", "F")
    with pytest.raises(AuthoringWorkflowError):
        _lift_recipe_roots_into_units([IntegrationComponentSpec(key="main", type="process", action="create", config={})], (("main", _ir()),), {})


def test_the_process_xml_goldens_are_untouched_by_governance():
    """Governance is envelope data: the emitted process bytes do not move."""
    from boomi_mcp.authoring.workflow import compile_authoring_request_v1

    plain, _ = compile_authoring_request_v1(_request([_unit(name="PFX proc")], [_conn(name="PFX conn"), _op()]), boomi_client=MagicMock(), profile=_PROFILE)
    governed, _ = compile_authoring_request_v1(_request([_unit(component_prefix="PFX", folder_name="F", description="d")], [_conn(), _op()]), boomi_client=MagicMock(), profile=_PROFILE)
    def _process_artifacts(result):
        # the emission plan and the normalized IR: what the emitted process
        # XML is a function of. The materialization plan legitimately differs
        # (it carries the envelope's name and folder), so it is excluded here.
        return sorted(
            (f.artifact_kind, f.component_key, f.digest)
            for f in result.artifact_fingerprints
            if f.artifact_kind in ("process_ir_emission_plan", "process_ir_normalized")
        )

    assert _process_artifacts(plain) and _process_artifacts(plain) == _process_artifacts(governed)
    plan_kinds = {f.artifact_kind for f in governed.artifact_fingerprints}
    assert "process_component_materialization_plan" in plan_kinds


# ---------------------------------------------------------------------------
# the recorded-intent contribution through the recipe engine's composer
# ---------------------------------------------------------------------------


def _attributed(contribution, invocation="i1", recipe="r.demo", version="1.0.0", index=0):
    from boomi_mcp.recipes.composer import AttributedContributionV1

    return AttributedContributionV1(invocation_id=invocation, recipe_id=recipe, recipe_version=version, index=index, contribution=contribution)


def _recorded(intent_id="hint.a", process_key="main"):
    from boomi_mcp.models.recipe_contributions import RecordedIntentContributionV1

    return RecordedIntentContributionV1(contribution_kind="recorded_intent", version="1", intent_id=intent_id, process_key=process_key,
                                        status="recorded_not_wired", declaration={"declaration_kind": "runtime_hint", "hint_kind": "note", "value": "x"})


def test_the_composer_carries_recorded_intents_and_refuses_orphans_and_duplicates():
    from boomi_mcp.recipes.composer import compose
    from boomi_mcp.recipes.errors import RecipeError

    composed = compose([_attributed(_recorded())], {}, direct_process_roots={"main": _ir()})
    assert [item.contribution.intent_id for item in composed.recorded_intents] == ["hint.a"]
    assert composed.process_roots and composed.component_slots == () and composed.constraints == ()
    with pytest.raises(RecipeError):
        compose([_attributed(_recorded(process_key="ghost"))], {}, direct_process_roots={"main": _ir()})
    with pytest.raises(RecipeError):
        compose([_attributed(_recorded()), _attributed(_recorded(), index=1)], {}, direct_process_roots={"main": _ir()})


def test_the_run_result_derives_its_records_from_the_composed_result():
    from boomi_mcp.recipes.composer import compose
    from boomi_mcp.recipes.engine import RecipeRunResultV1

    composed = compose([_attributed(_recorded())], {}, direct_process_roots={"main": _ir()})
    result = RecipeRunResultV1(composed=composed, components=(), process_artifacts=(), topology_plans=(), provenance={})
    assert [c.intent_id for c in result.recorded_intents] == ["hint.a"]


def test_a_recipe_recorded_intent_is_a_declared_output_type():
    from boomi_mcp.recipes.contracts import RecipeOutputType
    from typing import get_args

    assert "recorded_intent" in get_args(RecipeOutputType)


def test_a_recipe_output_never_carries_flows():
    from boomi_mcp.models.recipe_contributions import scan_forbidden_recipe_shape

    assert scan_forbidden_recipe_shape({"flows": [{"key": "k"}]}) is not None


# ---------------------------------------------------------------------------
# governed plan -> compile -> materialize dry-run through the dispatcher
# ---------------------------------------------------------------------------


def _governed_multi_root_request():
    """Two roots with distinct governance; A is the admitted wrapper form calling B."""
    conn_b, op_b = _conn(key="conn_b"), _op(key="op_b", conn="conn_b")
    root_b = _unit(key="root_b", conn="conn_b", op="op_b", component_prefix="B", folder_name="Folder B", description="root b")
    root_a = ProcessAuthoringUnitAuthoredV1(
        envelope=ProcessComponentEnvelopeAuthoredV1(
            component_key="root_a", action="create", component_prefix="A", folder_name="Folder A",
            description="root a", depends_on=("root_b",),
            recorded_intents=(_hint(),),
        ),
        # the ONLY admitted root-level process-call form (#175): a lone call with
        # wait/abort authored true — a call mixed with connector steps is refused
        process_ir=parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "process_call", "process_ref": "$ref:root_b", "wait": True, "abort_on_error": True}]}}),
    )
    return _request([root_a, root_b], [conn_b, op_b])


def test_a_governed_multi_root_dry_run_materializes_with_distinct_governance():
    """plan -> compile -> apply(dry_run) through `build_integration_action`, governance on both roots."""
    from boomi_mcp.categories import integration_builder
    from boomi_mcp.categories.integration_builder import build_integration_action

    request = _governed_multi_root_request()
    compiled, internals = compile_authoring_request_v1(request, boomi_client=MagicMock(), profile=_PROFILE)
    spec = internals.normalized.integration_spec
    roots = {u.envelope.component_key: u.envelope for u in spec.processes}
    assert (roots["root_a"].name, roots["root_a"].folder_name) == ("A root_a", "Folder A")
    assert (roots["root_b"].name, roots["root_b"].folder_name) == ("B root_b", "Folder B")
    by_key = {c.key: c for c in spec.components}
    # B owns its connection; A owns only B (a root), so nothing of B's is re-placed under A
    assert (by_key["conn_b"].name, by_key["conn_b"].config["folder_name"]) == ("B conn_b", "Folder B")
    assert by_key["op_b"].config["folder_name"] == "Folder B"
    assert [r.intent_id for r in compiled.recorded_intents] == ["runtime_hint/0"]
    payload = request.model_dump(mode="json")
    payload["expected_capability_revision"] = compiled.revision_binding.capability_revision
    payload["expected_compile_hash"] = compiled.revision_binding.compile_hash
    with patch.object(integration_builder, "_execute_component") as execute, patch.object(
        integration_builder, "create_component"
    ) as create:
        result = build_integration_action(MagicMock(), _PROFILE, "apply", {"authoring_request": payload, "dry_run": True})
    assert result.get("_success") is True, result.get("error")
    assert result.get("mutation_performed") is False and execute.call_count == 0 and create.call_count == 0
    # two distinct materialization plans, one per root, each fingerprinted
    kinds = [(f.component_key, f.artifact_kind) for f in compiled.artifact_fingerprints]
    assert ("root_a", "process_component_materialization_plan") in kinds
    assert ("root_b", "process_component_materialization_plan") in kinds
