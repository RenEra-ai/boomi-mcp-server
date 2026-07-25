"""ConnectorCall reference resolution and capability gating (issue #140, M12.5).

Covers the four resolution-phase codes plus the capability registry itself. The
FLOW-level codes (cardinality, map profile continuity) and the representative
mixed flow live in ``test_connector_call_mixed_flow.py``.

Everything here is DARK/internal: the modules under test are imported directly
and are deliberately absent from ``compiler.process_ir.__all__``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from boomi_mcp.errors import (
    ERROR_TAXONOMY,
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
)
from boomi_mcp.compiler.process_ir import connector_capabilities as caps
from boomi_mcp.compiler.process_ir.connector_resolution import (
    resolve_connector_call_bindings,
)
from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1, SymbolTableV1
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
from boomi_mcp.models.process_ir import parse_process_ir_v1

ISSUE_140_CODES = (
    PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
    PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
    PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
    PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
    PROCESS_IR_SEMANTIC_PROFILE_MISMATCH,
    PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH,
    PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID,
)


# ---------------------------------------------------------------------------
# Helpers — a minimal one-call flow, so each negative isolates ONE cause
# ---------------------------------------------------------------------------


def one_call_doc(operation_ref="op", action=None):
    step = {"kind": "connector_call", "operation_ref": operation_ref}
    if action is not None:
        step["action"] = action
    return {"version": "1", "body": {"kind": "sequence", "steps": [step, {"kind": "stop"}]}}


def symbols(*items):
    return SymbolTableV1(symbols=tuple(items))


def rest_get_symbols(**operation_overrides):
    """A resolvable REST GET entry call: connection + operation + response profile."""
    operation = dict(
        ref="op",
        component_id="id_op",
        component_type="connector-action",
        connector_type="rest",
        action_type="GET",
        connection_ref="conn",
        output_profile_ref="prof",
    )
    operation.update(operation_overrides)
    return symbols(
        ComponentSymbolV1(
            ref="conn",
            component_id="id_conn",
            component_type="connector-settings",
            connector_type="rest",
        ),
        ComponentSymbolV1(ref="prof", component_id="id_prof", component_type="profile.json"),
        ComponentSymbolV1(**operation),
    )


def compile_codes(doc, table):
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(parse_process_ir_v1(doc), table)
    return [(d.code, d.path, d.phase) for d in excinfo.value.diagnostics]


# ---------------------------------------------------------------------------
# The capability registry
# ---------------------------------------------------------------------------


def test_rest_family_constant_matches_the_builder_subtype():
    """The registry's REST key must be the builder's canonical subtype.

    The registry holds a LITERAL rather than importing the constant, so that
    ``import boomi_mcp.compiler`` does not drag in the 7k-line builder module.
    This pins the literal instead — the same trade the compiler already makes
    for the alias table.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        REST_CLIENT_SUBTYPE,
    )

    assert caps.REST_FAMILY == REST_CLIENT_SUBTYPE


def test_component_symbol_optional_field_set_is_pinned():
    """The symbol table is the compiler's resolution-context CONTRACT, so its
    field set may only change deliberately. Pinning it also keeps the
    architecture docs honest: #140's first QA round caught the doc claiming two
    ``*_profile_type`` fields that had been removed from the model."""
    required = {"ref", "component_id", "component_type"}
    optional = set(ComponentSymbolV1.model_fields) - required
    assert optional == {
        "connector_type",
        "action_type",
        "connection_ref",
        "input_profile_ref",
        "output_profile_ref",
    }
    # Every optional field must actually default, or a pre-#140 caller's symbol
    # construction would break.
    bare = ComponentSymbolV1(ref="r", component_id="c", component_type="connector-action")
    assert all(getattr(bare, name) is None for name in optional)


def test_registry_is_immutable_and_closed():
    with pytest.raises(TypeError):
        caps.CONNECTOR_CALL_CAPABILITIES_V1[("x", "y")] = None  # type: ignore[index]
    assert set(caps.CONNECTOR_CALL_CAPABILITIES_V1) == {
        (caps.REST_FAMILY, "get"),
        (caps.REST_FAMILY, "patch"),
        (caps.SOAP_FAMILY, "execute"),
        (caps.DATABASE_FAMILY, "get"),
        (caps.DATABASE_FAMILY, "send"),
    }


def test_database_send_is_the_only_non_producing_row():
    """The Send gate. Official docs: a Send action "does not return any data to
    the process for further processing", and Database (Legacy) declares only a
    Write profile (an INPUT). Everything else in the registry produces."""
    non_producing = [
        key
        for key, row in caps.CONNECTOR_CALL_CAPABILITIES_V1.items()
        if not row.produces_output
    ]
    assert non_producing == [(caps.DATABASE_FAMILY, "send")]


def test_consumers_cannot_be_entry_and_producers_can():
    table = caps.CONNECTOR_CALL_CAPABILITIES_V1
    assert table[(caps.DATABASE_FAMILY, "send")].accepts_input == "documents_required"
    assert table[(caps.REST_FAMILY, "patch")].accepts_input == "documents_required"
    for key in ((caps.REST_FAMILY, "get"), (caps.SOAP_FAMILY, "execute"), (caps.DATABASE_FAMILY, "get")):
        assert table[key].accepts_input == "none_or_documents"


@pytest.mark.parametrize(
    "alias,action,expected_family,expected_action",
    [
        ("rest", "get", caps.REST_FAMILY, "GET"),
        ("rest_client", "PATCH", caps.REST_FAMILY, "PATCH"),
        ("soap_client", "EXECUTE", caps.SOAP_FAMILY, "EXECUTE"),
        ("wssoapclientsdk", "EXECUTE", caps.SOAP_FAMILY, "EXECUTE"),
        # NOT upper-cased off the REST path: a database write emits the
        # mixed-case verb ``Send``, and ``SEND`` would be a different wire value.
        ("database", "Send", caps.DATABASE_FAMILY, "Send"),
        ("DATABASE", "Get", caps.DATABASE_FAMILY, "Get"),
    ],
)
def test_canonicalization_resolves_aliases_and_preserves_action_spelling(
    alias, action, expected_family, expected_action
):
    assert caps.canonicalize_connector_metadata(alias, action) == (
        expected_family,
        expected_action,
    )


def test_lookup_is_case_insensitive_on_the_action_only():
    assert caps.lookup_capability(caps.DATABASE_FAMILY, "send") is not None
    assert caps.lookup_capability(caps.DATABASE_FAMILY, "SEND") is not None
    # ...but never on the family: a family is an opaque account-scoped string.
    assert caps.lookup_capability(caps.DATABASE_FAMILY.upper(), "Send") is None


@pytest.mark.parametrize(
    "family,action",
    [
        # Every gated class fails by ABSENCE from the allowlist, not by being
        # remembered in a rejection list.
        ("wss", "LISTEN"),
        ("web_services_server", "LISTEN"),
        ("database_v2", "Send"),
        ("intappoemprod-X3ABOD-intapp-dev", "PATCH"),  # a live OEM REST subtype
        (caps.REST_FAMILY, "POST"),
        (caps.REST_FAMILY, "DELETE"),
        (caps.SOAP_FAMILY, "Send"),
        (caps.DATABASE_FAMILY, "Upsert"),
    ],
)
def test_gated_and_unknown_pairs_are_absent(family, action):
    assert caps.lookup_capability(family, action) is None


# ---------------------------------------------------------------------------
# Resolution — the happy path
# ---------------------------------------------------------------------------


def test_binding_derives_the_connection_from_the_operation_symbol():
    table = rest_get_symbols()
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(one_call_doc()))
    (binding,) = resolve_connector_call_bindings(cfg, table)
    assert binding.connection_ref == "conn"
    assert (binding.family, binding.action) == (caps.REST_FAMILY, "GET")
    assert binding.role == "entry"
    assert binding.capability.produces_output is True


def test_binding_is_identical_under_shuffled_symbol_insertion_order():
    """Determinism: ``SymbolTableV1`` canonicalises on ``ref``, so caller order
    must not be observable in a binding."""
    table = rest_get_symbols()
    shuffled = SymbolTableV1(symbols=tuple(reversed(table.symbols)))
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(one_call_doc()))
    assert resolve_connector_call_bindings(cfg, table) == resolve_connector_call_bindings(
        cfg, shuffled
    )


def test_the_same_operation_ref_always_yields_the_same_binding():
    table = rest_get_symbols()
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(one_call_doc()))
    first = resolve_connector_call_bindings(cfg, table)
    second = resolve_connector_call_bindings(cfg, table)
    assert first == second


def test_an_omitted_action_is_not_an_assertion():
    table = rest_get_symbols()
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(one_call_doc(action=None)))
    (binding,) = resolve_connector_call_bindings(cfg, table)
    assert binding.action == "GET"


def test_an_authored_action_matches_case_insensitively():
    table = rest_get_symbols()
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(one_call_doc(action="get")))
    (binding,) = resolve_connector_call_bindings(cfg, table)
    # The ASSERTION is case-insensitive; the EMITTED spelling stays authoritative.
    assert binding.action == "GET"


# ---------------------------------------------------------------------------
# Resolution — one negative per code
# ---------------------------------------------------------------------------


def test_unresolved_operation_ref():
    table = symbols(
        ComponentSymbolV1(ref="other", component_id="x", component_type="connector-action")
    )
    assert compile_codes(one_call_doc(), table) == [
        (
            PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND,
            "/body/steps/0/operation_ref",
            "reference_resolution",
        )
    ]


def test_operation_ref_resolving_to_a_non_operation_component():
    """A reference that resolves to the WRONG kind is not "found" — treating it
    as found would push the failure into the emitter with a worse message."""
    table = symbols(
        ComponentSymbolV1(ref="op", component_id="id", component_type="connector-settings")
    )
    assert compile_codes(one_call_doc(), table)[0][0] == (
        PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND
    )


def test_operation_declaring_no_connection():
    """The exact case that made the architect's original design unimplementable:
    no connector-action component declares its connection, so an operation symbol
    without one is a plan-level omission, reported as such."""
    table = rest_get_symbols(connection_ref=None)
    assert compile_codes(one_call_doc(), table) == [
        (
            PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND,
            "/body/steps/0/operation_ref",
            "reference_resolution",
        )
    ]


def test_connection_ref_that_resolves_to_nothing():
    table = rest_get_symbols(connection_ref="missing")
    assert compile_codes(one_call_doc(), table)[0][0] == (
        PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND
    )


def test_connection_of_the_wrong_component_type():
    table = symbols(
        ComponentSymbolV1(ref="conn", component_id="id_conn", component_type="transform.map"),
        ComponentSymbolV1(ref="prof", component_id="id_prof", component_type="profile.json"),
        ComponentSymbolV1(
            ref="op",
            component_id="id_op",
            component_type="connector-action",
            connector_type="rest",
            action_type="GET",
            connection_ref="conn",
            output_profile_ref="prof",
        ),
    )
    assert compile_codes(one_call_doc(), table)[0][0] == (
        PROCESS_IR_REFERENCE_CONNECTION_MISMATCH
    )


def test_connection_of_a_different_connector_family():
    """A REST operation bound to a database connection would emit XML naming one
    family while pointing at a connection of another."""
    table = symbols(
        ComponentSymbolV1(
            ref="conn",
            component_id="id_conn",
            component_type="connector-settings",
            connector_type="database",
        ),
        ComponentSymbolV1(ref="prof", component_id="id_prof", component_type="profile.json"),
        ComponentSymbolV1(
            ref="op",
            component_id="id_op",
            component_type="connector-action",
            connector_type="rest",
            action_type="GET",
            connection_ref="conn",
            output_profile_ref="prof",
        ),
    )
    assert compile_codes(one_call_doc(), table)[0][0] == (
        PROCESS_IR_REFERENCE_CONNECTION_MISMATCH
    )


def test_a_connection_that_omits_its_family_is_rejected():
    """FAIL-CLOSED. The emitter does not need the connection's family, but the
    VERIFICATION does: the emitted shape carries the OPERATION's family next to
    this connection's id, so an unverifiable binding would serialise a REST
    `connectorType` pointing at a database connection with nothing objecting.
    "Nothing to compare" is not the same as "compares equal"."""
    table = symbols(
        ComponentSymbolV1(
            ref="conn", component_id="id_conn", component_type="connector-settings"
        ),
        ComponentSymbolV1(ref="prof", component_id="id_prof", component_type="profile.json"),
        ComponentSymbolV1(
            ref="op",
            component_id="id_op",
            component_type="connector-action",
            connector_type="rest",
            action_type="GET",
            connection_ref="conn",
            output_profile_ref="prof",
        ),
    )
    assert compile_codes(one_call_doc(), table) == [
        (
            PROCESS_IR_REFERENCE_CONNECTION_MISMATCH,
            "/body/steps/0/operation_ref",
            "reference_resolution",
        )
    ]


def test_no_pre_140_symbol_can_reach_the_connection_family_requirement():
    """The requirement above tightens nothing that exists: #139's adapters put
    connector metadata only on the OPERATION requirement, and their symbols carry
    no ``connection_ref`` at all — so a legacy symbol never reaches this path."""
    from boomi_mcp.compiler.process_ir.legacy_adapters.contracts import (
        LegacySymbolRequirementV1,
    )

    assert "connection_ref" not in LegacySymbolRequirementV1.model_fields


@pytest.mark.parametrize(
    "connector_type,action_type",
    [
        ("wss", "LISTEN"),
        ("database_v2", "Send"),
        ("intappoemprod-X3ABOD-intapp-dev", "PATCH"),
        ("rest", "POST"),
        ("soap_client", "Send"),
    ],
)
def test_unsupported_family_action_pairs_are_rejected(connector_type, action_type):
    table = rest_get_symbols(connector_type=connector_type, action_type=action_type)
    codes = compile_codes(one_call_doc(), table)
    assert codes[0][0] == PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED
    assert codes[0][2] == "reference_resolution"


def test_the_capability_gate_is_settled_before_the_connection():
    """Deliberate ordering. An unsupported family/action is the COARSER failure,
    so it wins even when the connection is also broken — otherwise a caller
    trying an unsupported connector is told to go fix their connection wiring,
    which would not have helped."""
    table = rest_get_symbols(
        connector_type="wss", action_type="LISTEN", connection_ref="missing"
    )
    assert compile_codes(one_call_doc(), table)[0][0] == (
        PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED
    )
    # ...and within a SUPPORTED family, the connection failure is what surfaces.
    supported = rest_get_symbols(connection_ref="missing")
    assert compile_codes(one_call_doc(), supported)[0][0] == (
        PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND
    )


def test_missing_connector_metadata_on_the_operation_symbol():
    table = rest_get_symbols(connector_type=None, action_type=None)
    assert compile_codes(one_call_doc(), table)[0][0] == (
        PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED
    )


def test_an_authored_action_that_contradicts_the_operation_is_rejected():
    """The authored action can only ever REJECT — it never overrides the
    authoritative metadata, so a caller cannot re-label a GET as a PATCH."""
    table = rest_get_symbols()
    codes = compile_codes(one_call_doc(action="PATCH"), table)
    assert codes == [
        (
            PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED,
            "/body/steps/0/action",
            "reference_resolution",
        )
    ]


def test_the_unsupported_path_points_at_the_authored_action_when_there_is_one():
    """Path selection is not cosmetic: it sends the reader to the field they can
    actually change."""
    table = rest_get_symbols(connector_type="rest", action_type="POST")
    assert compile_codes(one_call_doc(action="POST"), table)[0][1] == "/body/steps/0/action"
    assert (
        compile_codes(one_call_doc(action=None), table)[0][1]
        == "/body/steps/0/operation_ref"
    )


# ---------------------------------------------------------------------------
# Security — ADR-001 §11
# ---------------------------------------------------------------------------


def test_no_authored_or_symbol_value_reaches_a_diagnostic_or_exception():
    """Seed a sentinel into every reachable slot and prove it appears nowhere in
    the diagnostics, the exception string, or any repr."""
    marker = "ZZSENTINELZZ"
    table = symbols(
        ComponentSymbolV1(
            ref=marker + "op",
            component_id=marker + "id",
            component_type="connector-action",
            connector_type=marker + "family",
            action_type=marker + "action",
            connection_ref=marker + "conn",
        )
    )
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compile_process_ir_v1(
            parse_process_ir_v1(one_call_doc(operation_ref=marker + "op", action=marker)),
            table,
        )
    error = excinfo.value
    haystack = [str(error), repr(error)]
    for item in error.diagnostics:
        haystack.extend(
            [item.code, item.path, item.node_identity, item.message, item.remediation, repr(item)]
        )
    assert marker not in "\n".join(haystack)


def test_binding_repr_suppresses_every_value_but_the_structural_ones():
    table = rest_get_symbols()
    cfg = lower_process_ir_to_cfg(parse_process_ir_v1(one_call_doc()))
    (binding,) = resolve_connector_call_bindings(cfg, table)
    text = repr(binding)
    assert "id_op" not in text and "id_conn" not in text
    assert "role=" in text  # structural fields still render


# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------


def test_all_seven_codes_are_registered_and_owned_by_140():
    for code in ISSUE_140_CODES:
        spec = ERROR_TAXONOMY[code]
        assert spec.owner == "#140"
        assert spec.category == "process_ir"
        assert spec.retryable is False


def test_the_new_codes_extend_existing_families_and_add_none():
    """ADR-001 §7 reserves exactly ten families; #140 ADDS codes to four that
    already exist and must not introduce an eleventh."""
    prefixes = {
        "PROCESS_IR_REFERENCE_",
        "PROCESS_IR_CAPABILITY_",
        "PROCESS_IR_SEMANTIC_",
        "PROCESS_IR_COMPILE_",
    }
    for code in ISSUE_140_CODES:
        assert any(code.startswith(prefix) for prefix in prefixes), code


def test_no_140_code_overwrote_an_earlier_owner():
    """``ERROR_TAXONOMY`` is a dict comprehension keyed on the code, so a
    duplicate entry would silently steal an existing family's owner."""
    for code, spec in ERROR_TAXONOMY.items():
        if spec.owner == "#140":
            assert code in ISSUE_140_CODES
