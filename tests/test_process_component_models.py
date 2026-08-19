"""Typed process component envelope / unit contracts (issue #153 / M12.15).

These models are the apply-essential half of a process root: everything the
existing apply machinery needs that ``ProcessIRV1`` deliberately does not carry.
The rules they enforce are not invented here — they MIRROR
``process_flow_builder._extract_process_extension_connections``, which is the
byte-authority for ``<bns:processOverrides>``. Where a rule below looks odd
(``label`` not stripped while ``id`` is; ``xpath`` mandatory only for an
explicitly ``database`` override) it is odd in the legacy reader too, and this
suite pins the correspondence so a future tidy-up of one cannot silently diverge
from the other.
"""

import copy
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.models.integration_models import (  # noqa: E402
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
    ProcessConnectionOverrideV1,
    ProcessExtensionBindingsV1,
    ProcessOverrideFieldV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

from _m12_11_support import VALID_IR_DOC  # noqa: E402


def _ir():
    return parse_process_ir_v1(copy.deepcopy(VALID_IR_DOC))


def _envelope(**overrides):
    kwargs = {"component_key": "root", "name": "My Process", "action": "create"}
    kwargs.update(overrides)
    return ProcessComponentEnvelopeV1(**kwargs)


def _unit(key="root", **envelope_overrides):
    return ProcessAuthoringUnitV1(
        envelope=_envelope(component_key=key, **envelope_overrides),
        process_ir=_ir(),
    )


# ---------------------------------------------------------------------------
# Required fields — `name` and `action` have NO defaults in this slice
# ---------------------------------------------------------------------------


def test_name_and_action_are_required_with_no_default():
    """#157 later derives default names; until then, guessing either is unsafe.

    A defaulted ``action`` would let a caller who meant ``update`` silently
    create a second component, and a defaulted ``name`` would let them overwrite
    or create one they never named. Both are mutation-accounting hazards, which
    is why the issue makes them required caller-supplied fields.
    """
    with pytest.raises(ValidationError) as missing_action:
        ProcessComponentEnvelopeV1(component_key="root", name="n")
    assert missing_action.value.errors()[0]["type"] == "missing"

    with pytest.raises(ValidationError) as missing_name:
        ProcessComponentEnvelopeV1(component_key="root", action="create")
    assert missing_name.value.errors()[0]["type"] == "missing"

    # Positive control: supplying both is accepted, so the assertions above
    # cannot be passing because the model rejects everything.
    assert _envelope().action == "create"


def test_action_is_a_closed_two_value_union():
    assert _envelope(action="update").action == "update"
    for bad in ("delete", "create_clone", "CREATE", ""):
        with pytest.raises(ValidationError):
            _envelope(action=bad)


@pytest.mark.parametrize("blank", ["", "   ", "\t"])
def test_blank_process_name_is_refused(blank):
    """The legacy assembler already refuses it; refuse it at authoring time."""
    with pytest.raises(ValidationError):
        _envelope(name=blank)


# ---------------------------------------------------------------------------
# Strictness: closed schemas and frozen instances
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ProcessOverrideFieldV1(id="i", label="l", nope=1),
        lambda: ProcessConnectionOverrideV1(
            connection_id="$ref:c",
            fields=(ProcessOverrideFieldV1(id="i", label="l"),),
            nope=1,
        ),
        lambda: ProcessExtensionBindingsV1(nope=1),
        lambda: _envelope(nope=1),
        lambda: ProcessAuthoringUnitV1(envelope=_envelope(), process_ir=_ir(), nope=1),
    ],
)
def test_every_new_model_rejects_unknown_fields(factory):
    with pytest.raises(ValidationError) as excinfo:
        factory()
    assert any(e["type"] == "extra_forbidden" for e in excinfo.value.errors())


def test_models_are_frozen():
    envelope = _envelope()
    with pytest.raises(ValidationError):
        envelope.name = "renamed"
    unit = _unit()
    with pytest.raises(ValidationError):
        unit.envelope = _envelope(component_key="other")


def test_repr_suppresses_authored_values_but_keeps_discriminants():
    """Secrets/security is a blocking class: a repr must not echo authored text.

    Mirrors ``models.process_ir._ProcessIRBase``. The discriminants stay visible
    because they are what makes a traceback readable.
    """
    text = repr(_envelope(name="Sentinel Name", description="Sentinel Description"))
    assert "Sentinel Name" not in text
    assert "Sentinel Description" not in text
    # Positive control: the suppression is selective, not a blanket redaction.
    assert "action='create'" in text
    assert "component_key='root'" in text


# ---------------------------------------------------------------------------
# Cardinality: exactly one envelope per root, exactly one root per unit
# ---------------------------------------------------------------------------


def test_a_unit_pairs_exactly_one_envelope_with_exactly_one_root():
    """The invariant is carried by the SHAPE, not by a check.

    There is no root list and no optional envelope, so "no envelope" and "two
    roots" are not expressible. This test pins that the shape stays that way —
    if either field ever gains a sequence type or a default, it fails here
    rather than at apply time.
    """
    unit = _unit()
    assert unit.envelope.component_key == "root"
    assert unit.process_ir.version == "1"

    for missing in ({"envelope": _envelope()}, {"process_ir": _ir()}):
        with pytest.raises(ValidationError) as excinfo:
            ProcessAuthoringUnitV1(**missing)
        assert excinfo.value.errors()[0]["type"] == "missing"

    fields = ProcessAuthoringUnitV1.model_fields
    assert fields["envelope"].is_required()
    assert fields["process_ir"].is_required()


def test_the_outer_frozen_model_does_not_deep_freeze_the_nested_ir():
    """Why normalization must RE-PARSE the nested root rather than trust it.

    ``frozen=True`` on the unit is shallow: it stops rebinding ``unit.process_ir``
    but does not make the ``ProcessIRV1`` graph itself immutable, and
    ``ProcessIRV1`` is ``extra="forbid"`` but NOT frozen. So an instance handed
    in by a caller may have been built by something other than a strict parse.
    This is the same reason the existing compile path re-parses from a dump, and
    the reason a plan may not fingerprint the object it was given.
    """
    unit = _unit()
    with pytest.raises(ValidationError):
        unit.process_ir = _ir()          # rebinding the attribute IS refused
    assert type(unit.process_ir).model_config.get("frozen") is not True


# ---------------------------------------------------------------------------
# depends_on: set semantics, canonicalized
# ---------------------------------------------------------------------------


def test_depends_on_is_canonicalized_to_sorted_order():
    """Declaration order is not meaning — the sorter reads an unordered edge set.

    Canonicalizing here is what makes a permuted ``depends_on`` produce the SAME
    materialization plan, and therefore the same relocatable fingerprint,
    instead of minting a different plan for an identical graph.
    """
    assert _envelope(depends_on=("z", "a", "m")).depends_on == ("a", "m", "z")
    assert (
        _envelope(depends_on=("a", "m", "z")).depends_on
        == _envelope(depends_on=("z", "m", "a")).depends_on
    )


def test_self_and_duplicate_dependencies_are_refused():
    with pytest.raises(ValidationError) as self_dep:
        _envelope(component_key="root", depends_on=("root",))
    assert self_dep.value.errors()[0]["type"] == "process_component_self_dependency"

    with pytest.raises(ValidationError) as dup:
        _envelope(depends_on=("a", "a"))
    assert dup.value.errors()[0]["type"] == "process_component_duplicate_dependency"


@pytest.mark.parametrize("field", ["component_id", "folder_name"])
@pytest.mark.parametrize("bad", [" padded", "padded ", ""])
def test_optional_structural_strings_reject_blank_or_padded(field, bad):
    with pytest.raises(ValidationError):
        _envelope(**{field: bad})


def test_optional_structural_strings_accept_none_and_clean_values():
    envelope = _envelope(component_id="abc-123", folder_name="Some/Folder")
    assert (envelope.component_id, envelope.folder_name) == ("abc-123", "Some/Folder")
    assert _envelope().component_id is None
    assert _envelope().folder_name is None


def test_there_is_no_public_folder_id_field():
    """Placement is authored by NAME; the resolved id is account-bound.

    A public ``folder_id`` would put an account-bound value in the caller's
    contract and, worse, invite it into the relocatable fingerprint. Apply
    resolves the name to an id and records that on the internal plan and the
    attestation instead.
    """
    assert "folder_id" not in ProcessComponentEnvelopeV1.model_fields


# ---------------------------------------------------------------------------
# Extension bindings — mirroring the legacy byte-authority
# ---------------------------------------------------------------------------


def test_field_id_is_canonicalized_but_label_is_deliberately_not():
    """The legacy reader's asymmetry, reproduced and pinned.

    ``id`` is structural, so a padded variant must not compare unequal to the
    same field authored cleanly. ``label`` is cosmetic and XML-escaped on
    emission, so its exact bytes are preserved — stripping it here would change
    emitted bytes relative to the legacy renderer and break parity.
    """
    field = ProcessOverrideFieldV1(id="user", label="  Padded Label  ")
    assert field.label == "  Padded Label  "

    with pytest.raises(ValidationError):
        ProcessOverrideFieldV1(id="  user  ", label="Label")
    with pytest.raises(ValidationError):
        ProcessOverrideFieldV1(id="user", label="   ")


def test_xpath_is_required_only_for_an_explicitly_database_override():
    """REST overrides are id-keyed and omit xpath; DB overrides are xpath-keyed.

    Critically, a field with no ``xpath`` is valid BY ITSELF without the entry
    declaring ``connector_type`` at all — a hand-authored REST override that
    omits it must still build.
    """
    rest = ProcessConnectionOverrideV1(
        connection_id="$ref:api_conn",
        fields=(ProcessOverrideFieldV1(id="password", label="Password"),),
    )
    assert rest.connector_type is None and rest.fields[0].xpath is None

    unknown_type = ProcessConnectionOverrideV1(
        connection_id="$ref:api_conn",
        connector_type="http",
        fields=(ProcessOverrideFieldV1(id="password", label="Password"),),
    )
    assert unknown_type.fields[0].xpath is None

    with pytest.raises(ValidationError) as db_missing:
        ProcessConnectionOverrideV1(
            connection_id="$ref:db_conn",
            connector_type="database",
            fields=(ProcessOverrideFieldV1(id="username", label="Username"),),
        )
    assert "xpath" in str(db_missing.value)

    ok = ProcessConnectionOverrideV1(
        connection_id="$ref:db_conn",
        connector_type="database",
        fields=(
            ProcessOverrideFieldV1(
                id="username",
                label="Username",
                xpath="DatabaseConnectionSettings/@username",
            ),
        ),
    )
    assert ok.fields[0].xpath == "DatabaseConnectionSettings/@username"


def test_connector_type_is_case_folded_and_that_is_byte_safe():
    """Folding is safe precisely because ``connector_type`` is never emitted.

    ``_emit_process_overrides`` keys the declaration by connection id + field id
    (Boomi's own keying) and carries ``connector_type`` only for downstream
    tooling, so folding it cannot move an emitted byte. What it does buy is that
    ``Database`` and ``DATABASE`` reach the same xpath rule and the same plan
    fingerprint.
    """
    for spelling in ("database", "Database", "DATABASE"):
        entry = ProcessConnectionOverrideV1(
            connection_id="$ref:db",
            connector_type=spelling,
            fields=(ProcessOverrideFieldV1(id="u", label="U", xpath="A/@u"),),
        )
        assert entry.connector_type == "database"


def test_connection_and_field_order_are_preserved_not_sorted():
    """Order is byte-relevant, so it must survive verbatim.

    ``_emit_process_overrides`` renders connections and fields in input order.
    Sorting either — the instinct that makes ``depends_on`` canonical — would
    silently move emitted XML bytes and break parity with the legacy renderer.
    """
    fields = tuple(
        ProcessOverrideFieldV1(id=f"f{i}", label=f"L{i}") for i in (3, 1, 2)
    )
    entry = ProcessConnectionOverrideV1(connection_id="$ref:c", fields=fields)
    assert [f.id for f in entry.fields] == ["f3", "f1", "f2"]

    bindings = ProcessExtensionBindingsV1(
        connections=(
            ProcessConnectionOverrideV1(connection_id="$ref:z", fields=fields),
            ProcessConnectionOverrideV1(connection_id="$ref:a", fields=fields),
        )
    )
    assert [c.connection_id for c in bindings.connections] == ["$ref:z", "$ref:a"]


def test_duplicate_field_ids_are_preserved_because_the_renderer_preserves_them():
    """Parity outranks tidiness: the legacy renderer emits duplicates as given."""
    entry = ProcessConnectionOverrideV1(
        connection_id="$ref:c",
        fields=(
            ProcessOverrideFieldV1(id="same", label="First"),
            ProcessOverrideFieldV1(id="same", label="Second"),
        ),
    )
    assert [f.label for f in entry.fields] == ["First", "Second"]


def test_a_connection_override_must_declare_at_least_one_field():
    with pytest.raises(ValidationError) as excinfo:
        ProcessConnectionOverrideV1(connection_id="$ref:c", fields=())
    assert (
        excinfo.value.errors()[0]["type"] == "process_component_cardinality_invalid"
    )


def test_empty_bindings_are_a_valid_no_op():
    """Matches the legacy reader's treatment of an explicitly empty list."""
    assert ProcessExtensionBindingsV1().connections == ()
    assert _envelope().process_extensions.connections == ()


def test_connection_id_must_be_an_exact_reference_token():
    """The typed surface is strict where the legacy reader stripped.

    ``ComponentRefV1`` refuses surrounding whitespace outright. The legacy
    adapter normalizes before constructing these models, so no legacy input
    changes behaviour — but a caller authoring the typed surface directly gets
    told, rather than silently canonicalized.
    """
    assert (
        ProcessConnectionOverrideV1(
            connection_id="$ref:conn",
            fields=(ProcessOverrideFieldV1(id="i", label="l"),),
        ).connection_id
        == "$ref:conn"
    )
    for bad in (" $ref:conn", "$ref:conn ", ""):
        with pytest.raises(ValidationError):
            ProcessConnectionOverrideV1(
                connection_id=bad,
                fields=(ProcessOverrideFieldV1(id="i", label="l"),),
            )


# ---------------------------------------------------------------------------
# The shared component / process key namespace on IntegrationSpecV1
# ---------------------------------------------------------------------------


def test_processes_defaults_to_empty_and_is_additive():
    spec = IntegrationSpecV1(name="Spec")
    # A TUPLE since §6 AR1-08 (the plan's verbatim field type): list input still
    # coerces, so the wire is unchanged; in-place mutation is unrepresentable.
    assert spec.processes == ()
    assert "processes" in spec.model_dump()
    coerced = IntegrationSpecV1(name="Spec", processes=[])
    assert coerced.processes == ()


def test_a_key_may_not_be_used_by_both_a_component_and_a_process():
    with pytest.raises(ValidationError) as excinfo:
        IntegrationSpecV1(
            name="Spec",
            components=[IntegrationComponentSpec(key="shared", type="process")],
            processes=[_unit("shared")],
        )
    assert (
        excinfo.value.errors()[0]["type"] == "integration_component_key_duplicate"
    )


def test_a_process_key_may_not_be_declared_twice():
    with pytest.raises(ValidationError) as excinfo:
        IntegrationSpecV1(name="Spec", processes=[_unit("dup"), _unit("dup")])
    assert (
        excinfo.value.errors()[0]["type"] == "integration_component_key_duplicate"
    )


def test_the_namespace_check_does_not_strictify_the_legacy_component_surface():
    """Adding ``processes`` must not change a spec that carries none.

    A components-only duplicate has always been raised by
    ``integration_builder._topological_order``, not by the model. Moving that
    check into the model would make previously-constructible legacy specs fail
    at construction — a silent strictification of the legacy authoring surface,
    which is exactly what an additive field must not cause.
    """
    spec = IntegrationSpecV1(
        name="Spec",
        components=[
            IntegrationComponentSpec(key="same", type="process"),
            IntegrationComponentSpec(key="same", type="process"),
        ],
    )
    assert len(spec.components) == 2


def test_a_spec_carrying_processes_survives_a_model_dump_round_trip():
    """Apply and verify both REBUILD the spec from a dump.

    ``_apply_plan`` and ``_verify_build`` reconstruct ``IntegrationSpecV1`` from
    a stored dump, so a ``processes`` entry that failed to round-trip would
    vanish between plan and apply and the roots would silently not be
    materialized — a data-loss shaped failure with no error.
    """
    spec = IntegrationSpecV1(
        name="Spec",
        components=[IntegrationComponentSpec(key="conn", type="connector-settings")],
        processes=[_unit("root", depends_on=("conn",))],
    )
    assert IntegrationSpecV1(**spec.model_dump()) == spec

    from_json = IntegrationSpecV1(**spec.model_dump(mode="json"))
    assert len(from_json.processes) == 1
    assert from_json.processes[0].envelope.depends_on == ("conn",)


def test_every_structural_string_refuses_padding_on_the_same_footing():
    """§6 AR2-06: `name` accepted padding while its siblings refused it.

    The name is fingerprint-covered AND emitted into the component XML, so
    `"  N  "` and `"N"` — one canonical envelope by the plan's definition —
    minted different plan fingerprints and different submitted bytes. The
    module's rule is fail-closed rejection, and the fields are enumerated from
    the model itself rather than hand-listed, so a new structural string cannot
    quietly opt out.
    """
    import pydantic
    import pytest as _pytest

    from boomi_mcp.models.process_component import ProcessComponentEnvelopeV1

    def _envelope(**over):
        kwargs = {"component_key": "proc", "name": "N", "action": "create"}
        kwargs.update(over)
        return ProcessComponentEnvelopeV1(**kwargs)

    # The control: unpadded values construct.
    assert _envelope().name == "N"

    for field, padded in (
        ("name", "  N  "),
        ("component_key", "  proc  "),
        ("folder_name", "  F  "),
        ("component_id", "  cid  "),
    ):
        with _pytest.raises(pydantic.ValidationError) as caught:
            _envelope(**{field: padded})
        assert "whitespace" in str(caught.value) or "blank" in str(caught.value), (
            field, caught.value,
        )
