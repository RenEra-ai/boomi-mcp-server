"""The positive replay path, end to end (#155 slice F).

Slice D built the grant minter, the discovery surface and the apply-boundary
rechecks, and recorded the gap this file closes: no production path constructed
an idempotency contract symbol, so a caller could discover the correct reference
and planning would still refuse it. `ARCH-155-r10-03` says so in as many words —
"adding genuine records in the evidence slice will not by itself make a candidate
usable".

So this asserts the thing the manifest flip claims: given a registry record the
trusted snapshot can place, a retried conditionally-idempotent write COMPILES.
Every negative beside it is what keeps that from being a rubber stamp.
"""

from __future__ import annotations

_TESTS_ROOT_FOR_CLIENT = __import__('pathlib').Path(__file__).resolve().parent


def _evidenced_client():
    """A client that REPORTS the capture's account — see `evidenced_account_client`.

    Every witness here used a bare `MagicMock()`, whose account attribute is a
    Mock rather than a string, so the account reader found none and the scope
    check was skipped rather than failed. That is the fail-open arm the
    issue-level architect gate found, and these witnesses are why it stayed
    invisible: they proved the evidenced path in the one configuration where the
    check did not run.
    """
    import sys
    sys.path.insert(0, str(_TESTS_ROOT_FOR_CLIENT))
    from _m12_11_support import evidenced_account_client

    return evidenced_account_client()


import sys
from pathlib import Path

import pytest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _wave_gate_golden_corpus import error_symbols  # noqa: E402


def _symbols():
    """The corpus table plus a PATCH operation carrying LIVE-SHAPED identifiers.

    The shared fixture's component ids are readable placeholders — `op-patch`,
    `conn-1` — and a registry record cannot name one, because the record model
    validates its identity as a real Boomi component id. So corroboration would
    compare the record's id against a placeholder and refuse every time, and the
    positive path would be untestable on the shared symbols. These two symbols
    exist so the identity comparison has something it CAN match; the rest of the
    table is the corpus's.
    """
    from boomi_mcp.compiler.process_ir.connector_capabilities import REST_FAMILY
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1

    return error_symbols(
        ComponentSymbolV1(
            ref="$ref:PATCHLIVE",
            component_id=_OP_ID,
            component_type="connector-action",
            connector_type=REST_FAMILY,
            action_type="PATCH",
            connection_ref="$ref:CONNLIVE",
            input_profile_ref="$ref:P1",
            output_profile_ref="$ref:P2",
        ),
        ComponentSymbolV1(
            ref="$ref:CONNLIVE",
            component_id=_CONN_ID,
            component_type="connector-settings",
            connector_type=REST_FAMILY,
        ),
    )
from _process_ir_capability_witnesses import _connector_scope, _parse  # noqa: E402

from boomi_mcp.authoring.connector_resolution_snapshot import (  # noqa: E402
    ResolvedConnectorComponentIdentityV1,
    TrustedConnectorResolutionSnapshotV1,
)
from boomi_mcp.compiler.process_ir.connector_resolution import (  # noqa: E402
    project_grants_for_root,
    project_idempotency_contracts,
)
from boomi_mcp.connector_replay.models import (  # noqa: E402
    CaptureReferenceV1,
    LiveComponentIdentityV1,
    OperationContractRecordV1,
    StaticRouteCoverageV1,
)

_CONTRACT = "$ref:CONTRACT"
_DIGEST = "b" * 64
_ACCOUNT = "account-under-test"


def _scope():
    """The scope THIS account hashes to, from the repository's own helper.

    A literal here would make the record describe a different account, and the
    account comparison fails closed — so the contract would project and the grant
    would silently not follow, which is exactly the mechanism live QA flagged for
    the operation record. Deriving it means this test exercises that comparison
    rather than stepping around it.
    """
    from boomi_mcp.connector_replay.digests import account_scope_hash

    return account_scope_hash(_ACCOUNT)


_SCOPE = _scope()


_OP_ID = "11111111-2222-3333-4444-555555555555"
_CONN_ID = "66666666-7777-8888-9999-aaaaaaaaaaaa"
_CFG = "ComponentConfigDigestV1:" + "d" * 64


def _capture():
    """DERIVED from a real attested capture, never hand-built.

    The record's nested shapes — the capture reference, its closed observation
    set, the route coverage — are the schema's, and typing them out here would be
    a hand-model of exactly the sort this issue has recorded thirty-one instances
    of. It would also drift the day the schema gains a field. So the reference
    comes from the archived PATCH double execution, summarised by the ingester's
    own helper: the fixture's provenance is an executed capture, which is what
    this repository's fixture rule requires.
    """
    from boomi_mcp.connector_replay.capture import summarize
    from boomi_mcp.connector_replay.ingest import _capture_reference
    from boomi_mcp.connector_replay.models import SideEffectV1

    root = Path(__file__).resolve().parents[1]
    directory = (root / "docs/architecture/evidence/issue-155/captures"
                 / "cap155-e5-patch-attested")
    return _capture_reference(summarize(directory, method_hint="PATCH"),
                              SideEffectV1.WRITE)


def _route_digest():
    """A well-formed route digest, DERIVED from archived component bytes.

    Computed by the repository's Route Digest helper from the archived REST
    connection readback and the archived PATCH operation. A digest typed here
    would be a claim about a route nobody derived.

    The pairing is a fixture convenience and is stated rather than hidden: this
    test measures which records the projection can PLACE, and placement is
    decided by component identity, never by route coverage. The record needs a
    well-formed coverage value; it does not need one describing these captures'
    relationship to each other.
    """
    from boomi_mcp.connector_replay.digests import route_digest_v1

    captures = (Path(__file__).resolve().parents[1]
                / "docs/architecture/evidence/issue-155/captures")
    return route_digest_v1(
        (captures / "cap155-e1-conn-readback" / "rest-conn-c4281346.xml")
        .read_text(encoding="utf-8"),
        (captures / "cap155-e5-patch-attested" / "component_op_tgt.xml")
        .read_text(encoding="utf-8"),
    )


def _record(*, operation_id=_OP_ID, operation_version=7,
            connection_id=_CONN_ID, connection_version=3,
            contract_ref=_CONTRACT):
    return OperationContractRecordV1(
        contract_ref=contract_ref,
        family="rest",
        action="PATCH",
        semantics_id="ICV1",
        semantics_revision=1,
        account_scope_hash=_SCOPE,
        operation_identity=LiveComponentIdentityV1(
            component_id=operation_id, version=operation_version,
            config_digest=_CFG),
        connection_identity=LiveComponentIdentityV1(
            component_id=connection_id, version=connection_version,
            config_digest=_CFG),
        route_coverage=StaticRouteCoverageV1(route_digests=(_route_digest(),)),
        capture=_capture(),
        record_digest=_DIGEST,
    )


class _Registry:
    """Only the operation records are injected; everything else is the real one.

    The corroboration translates the compiler's platform connector type into the
    registry's logical family through `family_for`, so a stand-in that answers
    only `operation_records` makes every corroboration fail for a reason that has
    nothing to do with the record — which is exactly what the first version of
    this fixture did, and it read as "the evidence does not cover this call".
    """

    def __init__(self, *records):
        from boomi_mcp.connector_replay.registry import load_registry

        self.operation_records = tuple(records)
        self._packaged = load_registry()

    def __getattr__(self, name):
        return getattr(self._packaged, name)


def _snapshot(*, operation_version="7", connection_version="3"):
    """What slice C's trusted reading of the account observed.

    CARRIES `mode` AND a config digest, because a real reading does. The first
    version of this fixture supplied only an id and a version, and the placement
    accepted it — so the test proved placement on a reading the production
    snapshot cannot produce. Review found the same hole in the code: an identity
    with an id but no readable configuration says the component EXISTS, not that
    this plan runs the one the record observed.
    """
    return TrustedConnectorResolutionSnapshotV1(
        identities=(
            ResolvedConnectorComponentIdentityV1(
                component_key="PATCHLIVE", component_id=_OP_ID,
                component_version=operation_version, family="rest", action="PATCH",
                mode="reuse", config_digest=_CFG, live_read_failed=False,
            ),
            ResolvedConnectorComponentIdentityV1(
                component_key="CONNLIVE", component_id=_CONN_ID,
                component_version=connection_version, family="rest",
                mode="reuse", config_digest=_CFG, live_read_failed=False,
            ),
        ),
        account_scope=_ACCOUNT,
    )


def _doc(evidence=None):
    return _parse(_connector_scope(
        protected="$ref:PATCHLIVE",
        retry={"count": 2},
        idempotency=evidence,
    ))


def test_a_record_the_snapshot_can_place_mints_a_contract():
    projected = project_idempotency_contracts(
        _symbols(), registry=_Registry(_record()), snapshot=_snapshot())
    minted = [c for c in projected.idempotency_contracts]
    assert [(c.ref, c.operation_ref, c.record_digest) for c in minted] == [
        (_CONTRACT, "$ref:PATCHLIVE", _DIGEST)
    ], minted


@pytest.mark.parametrize(
    "kwargs,why",
    [
        (dict(operation_version=9),
         "the account serves the operation at another version"),
        (dict(connection_version=9),
         "the account serves the connection at another version"),
        (dict(operation_id="bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
         "no symbol carries the operation the record names"),
        (dict(connection_id="cccccccc-dddd-eeee-ffff-000000000000"),
         "no symbol carries the connection the record names"),
    ],
)
def test_a_record_the_snapshot_cannot_place_mints_nothing(kwargs, why):
    """Each way a record can fail to describe THIS plan, driven separately.

    Identity is the PAIR of component and version on BOTH sides, which is the
    rule discovery already applies: a record minted against one version does not
    describe a component the account now serves at another.
    """
    projected = project_idempotency_contracts(
        _symbols(), registry=_Registry(_record(**kwargs)), snapshot=_snapshot())
    assert projected.idempotency_contracts == (), why


def test_an_empty_registry_leaves_the_table_untouched():
    symbols = _symbols()
    assert project_idempotency_contracts(
        symbols, registry=_Registry(), snapshot=_snapshot()) is symbols


def test_the_production_entry_projects_contracts_before_grants():
    """The wiring, at the entry every production path already calls.

    Asserted through `project_grants_for_root` rather than the projection alone,
    because a projection nothing calls is the defect this slice exists to close —
    and slice D's own review found that gate wired-but-inert twice.
    """
    table = project_grants_for_root(
        _doc({"kind": "key_reference", "contract_ref": _CONTRACT}),
        _symbols(),
        process_root_ref="$ref:ROOT",
        registry=_Registry(_record()),
        snapshot=_snapshot(),
    )
    assert [c.ref for c in table.idempotency_contracts] == [_CONTRACT]
    assert table.idempotency_grants, (
        "contracts were projected but no grant followed, so the per-call gate "
        "would still refuse the write this record authorises"
    )


def _compile_errors(evidence, *, registry, snapshot):
    from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import (
        validate_process_ir,
    )

    doc = _doc(evidence)
    table = project_grants_for_root(
        doc, _symbols(), process_root_ref="$ref:ROOT",
        registry=registry, snapshot=snapshot,
    )
    return [(e.code, e.path) for e in validate_process_ir(doc, table).errors]


def test_an_evidenced_retried_write_compiles_and_an_unevidenced_one_does_not():
    """THE CLAIM THE MANIFEST FLIP RESTS ON, asserted end to end.

    Before this slice the chain stopped two layers short of the compiler and did
    so silently. Measured at the time: with all seven verb rows ingested, the
    registry answered `conditionally_idempotent` for a REST PATCH and the
    compiler refused the very same call as a non-idempotent write, because its
    capability table carried a hand-written `unverified` and never asked. Then,
    with the capability derived, it refused for evidence instead — the contract
    existed but no production path had ever built one.

    So this asserts the whole path in one place: attested capture, ingested row,
    capability derived from the observation, contract projected from the registry
    record, per-call grant minted, compile permitted. And beside it the three
    ways it must still refuse, because a gate that only ever says yes is not a
    gate.
    """
    registry, snapshot = _Registry(_record()), _snapshot()

    assert _compile_errors(
        {"kind": "key_reference", "contract_ref": _CONTRACT},
        registry=registry, snapshot=snapshot,
    ) == [], "an evidenced retried write must compile"

    # NO EVIDENCE AUTHORED. The capability is now observed, which is exactly when
    # a missing contract must still refuse — otherwise deriving the capability
    # would have turned an evidence gate into a permission.
    assert _compile_errors(None, registry=registry, snapshot=snapshot) == [
        ("PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING",
         "/body/steps/1/try_body/steps/0/idempotency")
    ]

    # EVIDENCE NAMING A CONTRACT NOBODY MINTED.
    assert _compile_errors(
        {"kind": "key_reference", "contract_ref": "$ref:NOSUCH"},
        registry=registry, snapshot=snapshot,
    ) == [("PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING",
           "/body/steps/1/try_body/steps/0/idempotency")]

    # AND WITH NO RECORD AT ALL the same document refuses, which is the state the
    # registry shipped in and the one every account without this evidence is in.
    assert _compile_errors(
        {"kind": "key_reference", "contract_ref": _CONTRACT},
        registry=_Registry(), snapshot=snapshot,
    ) == [("PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING",
           "/body/steps/1/try_body/steps/0/idempotency")]


def test_evidence_may_not_weaken_a_capability_the_transport_defines():
    """One-way, and asserted rather than assumed.

    An observation may replace `unverified` — the value meaning nobody has
    classified this action. It may not touch `read_only`, which the table derives
    from the transport's own definition of a safe method. Without this, a capture
    of a GET that happened to move state would rewrite a transport guarantee into
    a replay verdict.
    """
    from boomi_mcp.compiler.process_ir.connector_capabilities import (
        REST_FAMILY,
        lookup_capability,
    )

    read_only = lookup_capability(REST_FAMILY, "GET")
    assert read_only is not None and read_only.retry_safety == "read_only", read_only

    # THE RESTRICTION DRIVEN, not merely stated. No read-only action currently
    # has a positive row, so removing the restriction changes nothing today and a
    # test asserting `read_only` stays `read_only` grades an unreachable branch —
    # measured: with the clause deleted the whole file still passed. This makes
    # the case reachable by having the registry report a positive verdict for the
    # safe method, which is exactly the day the restriction has to hold.
    import boomi_mcp.compiler.process_ir.connector_capabilities as CC
    from boomi_mcp.connector_replay.models import RetrySafetyV1

    class _Loud:
        def __init__(self, packaged):
            self._packaged = packaged

        def retry_safety(self, family, action):
            return RetrySafetyV1.CONDITIONALLY_IDEMPOTENT

        def __getattr__(self, name):
            return getattr(self._packaged, name)

    from boomi_mcp.connector_replay import registry as _reg_module

    packaged = _reg_module.load_registry()
    original = _reg_module.load_registry
    _reg_module.load_registry = lambda: _Loud(packaged)
    try:
        assert CC.lookup_capability(REST_FAMILY, "GET").retry_safety == "read_only", (
            "an observation rewrote a safety the transport defines"
        )
        assert (CC.lookup_capability(REST_FAMILY, "PATCH").retry_safety
                == "conditionally_idempotent"), "the open value did not move"
    finally:
        _reg_module.load_registry = original

    # ...and the value that IS open to evidence has actually moved, so the test
    # above is a restriction rather than a description of a function that never
    # changes anything.
    observed = lookup_capability(REST_FAMILY, "PATCH")
    assert observed is not None and observed.retry_safety == "conditionally_idempotent", (
        "the packaged evidence no longer reaches the capability table; the whole "
        "chain this slice built is inert again"
    )


def test_the_capability_derivation_fails_closed_without_hiding_a_defect():
    """Three outcomes, each driven, because the handler's WIDTH is the property.

    The derivation reads a packaged registry, so it needs a fallback for a build
    that cannot read one. The first version caught `Exception`, which is a
    superclass of the `AssertionError` the #149 transport guard raises when a
    served producer reaches the platform — a genuine layering violation would
    have become a silently un-derived row. Narrowing it to the builtin error
    families then swung the other way: `RegistryInvalid` derives straight from
    `Exception`, so a malformed packaged registry would have crashed the compiler
    instead of falling back. Both were found by checking rather than by reading.
    """
    import boomi_mcp.compiler.process_ir.connector_capabilities as CC
    import boomi_mcp.connector_replay.registry as registry_module

    def _raising(exc):
        def _load():
            raise exc
        return _load

    original = registry_module.load_registry
    try:
        # 1. EVIDENCE PRESENT — the value moves. Without this the two fallbacks
        #    below would pass on a derivation that never fires.
        assert CC.lookup_capability(CC.REST_FAMILY, "PATCH").retry_safety == (
            "conditionally_idempotent"
        )

        # 2. REGISTRY UNREADABLE — fall back to the table, do not crash.
        registry_module.load_registry = _raising(
            registry_module.RegistryInvalid("malformed"))
        assert CC.lookup_capability(CC.REST_FAMILY, "PATCH").retry_safety == "unverified"

        # 3. A DEFECT IN THIS REPOSITORY — surface it. The transport guard's own
        #    exception type stands in for "something that must never be hidden".
        registry_module.load_registry = _raising(
            AssertionError("the derivation reached the Boomi transport"))
        with pytest.raises(AssertionError, match="reached the Boomi transport"):
            CC.lookup_capability(CC.REST_FAMILY, "PATCH")
    finally:
        registry_module.load_registry = original


def test_the_compile_route_can_place_a_record_for_a_component_the_author_names():
    """The REACH defect live QA found, and the narrow fix for it.

    QA drove five plan shapes through the public compile route: every one
    resolved create-mode with no component identity, so no registry record could
    be placed and an evidenced retried write refused for want of evidence it had
    no way to present. The identical code at apply placed the record correctly —
    the mechanism was right and its POSITION was wrong.

    The fix reads live identities on the compile route for components the author
    has explicitly NAMED, and only those. This asserts both halves: the named
    component's identity reaches the snapshot and the record is placed, and a
    plan that names nothing reads nothing — which is what keeps slice C's
    deferred-live-read decision intact for every plan that creates its
    components.
    """
    from unittest.mock import MagicMock

    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
        live_readings_for_declared_components,
    )
    from boomi_mcp.compiler.process_ir.connector_capabilities import REST_FAMILY
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.connector_replay.registry import load_registry

    record = load_registry().operation_records[0]
    operation, connection = record.operation_identity, record.connection_identity

    class _Component:
        def __init__(self, key, component_id, component_type, connector_type):
            self.key = key
            self.component_id = component_id
            self.type = component_type
            self.component_type = component_type
            self.connector_type = connector_type
            self.config = {"connector_type": "rest", "method": record.action}

    named = [
        _Component("PATCHLIVE", operation.component_id, "connector-action", REST_FAMILY),
        _Component("CONNLIVE", connection.component_id, "connector-settings", REST_FAMILY),
    ]
    reads = []

    # THE ARCHIVED COMPONENT BYTES, not a stub. A placeholder document carries
    # no readable configuration, so the snapshot derives no config digest and the
    # placement — correctly — refuses it. Using the real bytes is what makes this
    # test exercise the path a caller takes rather than a shape only a test
    # produces.
    captures = (Path(__file__).resolve().parents[1]
                / "docs/architecture/evidence/issue-155/captures"
                / "cap155-e7-patch-operation-record")
    operation_xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")

    def _get_xml(_client, component_id, *_a, **_k):
        reads.append(component_id)
        is_operation = component_id == operation.component_id
        return {
            "component_id": component_id,
            "version": operation.version if is_operation else connection.version,
            "xml": operation_xml if is_operation else connection_xml,
        }

    with patch("boomi_mcp.categories.components._shared.component_get_xml", _get_xml):
        readings = live_readings_for_declared_components(_evidenced_client(), named)
        # A PLAN THAT NAMES NOTHING READS NOTHING — the cost invariant, asserted
        # beside the positive so it cannot quietly regress.
        before = len(reads)
        creates = [_Component("NEW", None, "connector-action", REST_FAMILY)]
        assert live_readings_for_declared_components(_evidenced_client(), creates) == {}
        assert len(reads) == before, "a create-only plan reached the platform"

    # STRINGS, which is the identity model's contract: the platform reports an
    # integer and the reader normalises it. Asserting the raw integer here is
    # what let the un-normalised version reach the public entry and raise.
    assert {k: v["component_version"] for k, v in readings.items()} == {
        "PATCHLIVE": str(operation.version), "CONNLIVE": str(connection.version)
    }

    snapshot = build_connector_resolution_snapshot(
        named, declared={}, live_component_xml=readings,
        reused_keys=tuple(readings),
    )
    # Stringified on BOTH sides, which is what the projection itself compares —
    # the snapshot carries the version as the platform reported it and the record
    # as the model typed it, and asserting one form against the other would fail
    # a placement that is correct.
    placed = {i.component_key: (i.component_id, str(i.component_version))
              for i in snapshot.identities}
    assert placed.get("PATCHLIVE") == (
        operation.component_id, str(operation.version)
    ), placed

    symbols = error_symbols(
        ComponentSymbolV1(
            ref="$ref:PATCHLIVE", component_id=operation.component_id,
            component_type="connector-action", connector_type=REST_FAMILY,
            action_type=record.action, connection_ref="$ref:CONNLIVE",
            input_profile_ref="$ref:P1", output_profile_ref="$ref:P2",
        ),
        ComponentSymbolV1(
            ref="$ref:CONNLIVE", component_id=connection.component_id,
            component_type="connector-settings", connector_type=REST_FAMILY,
        ),
    )
    projected = project_idempotency_contracts(
        symbols, registry=None, snapshot=snapshot)
    assert [c.ref for c in projected.idempotency_contracts] == [record.contract_ref], (
        "the compile-route snapshot still cannot place the packaged record"
    )


def test_the_public_compile_entry_reads_a_named_component_and_stringifies_it():
    """Driven at the PUBLIC entry, because a hand-built snapshot already fooled me.

    The withdrawn capability flip rested on an admission witness that constructed
    its own snapshot, and live QA named that as the defect: it is the one object
    the public compile path cannot produce. So this drives
    `compile_authoring_request_v1` itself.

    It also pins the bug that only the public entry could reveal. The identity
    model types the component version as a string and the platform reports an
    integer; the reader passed it through raw and every hand-built check still
    passed, while the real entry raised a validation error on the first named
    component.
    """
    from unittest.mock import MagicMock

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.connector_replay.registry import load_registry

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _m12_11_support import (  # noqa: E402
        APPLIABLE_CONN,
        APPLIABLE_OP,
        appliable_process_ir_request,
    )

    record = load_registry().operation_records[0]
    operation, connection = record.operation_identity, record.connection_identity
    reads = []

    def _get_xml(_client, component_id, *_a, **_k):
        reads.append(component_id)
        version = (operation.version if component_id == operation.component_id
                   else connection.version)
        # The PLATFORM's shape: an integer version, which is what broke this.
        return {"component_id": component_id, "version": version, "xml": "<x/>"}

    request = appliable_process_ir_request(components=(
        dict(APPLIABLE_CONN, component_id=connection.component_id, action="update"),
        dict(APPLIABLE_OP, component_id=operation.component_id, action="update"),
    ))
    with patch("boomi_mcp.categories.components._shared.component_get_xml", _get_xml), \
         patch("boomi_mcp.categories.integration_builder.paginate_metadata",
               lambda *a, **k: []):
        result, _internals = compile_authoring_request_v1(
            request, boomi_client=_evidenced_client(), profile="qa")

    assert result is not None
    assert sorted(reads) == sorted(
        [operation.component_id, connection.component_id]
    ), reads


def test_an_authored_reference_only_reuse_compiles_a_retried_evidenced_write():
    """THE CALLER-AUTHORABLE POSITIVE PATH, end to end at the public entry.

    This is the assertion the capability manifest rests on, and it took three
    attempts to be able to write it honestly. The first two admission witnesses
    proved shapes I had constructed — a hand-built snapshot, then a component
    form the authoring surface refuses — and each passed without ever reaching
    the evidence question. Review supplied the shape that does reach it: the
    documented `action="create"` with `config.reference_only=true`, which is how
    a caller says "use the component that already exists".

    Everything below is packaged or archived: the registry record, the component
    bytes, the semantics. Only the network boundary is faked, and it returns the
    platform's own shapes. The document is a retried PATCH carrying a
    `key_reference` naming the packaged contract — the thing that refused for
    want of evidence at every earlier point in this slice.
    """
    from unittest.mock import MagicMock

    from boomi_mcp.authoring.workflow import compile_authoring_request_v1
    from boomi_mcp.compiler.process_ir import connector_resolution as CR
    from boomi_mcp.connector_replay.registry import load_registry

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _m12_11_support import (  # noqa: E402
        APPLIABLE_CONN,
        APPLIABLE_OP,
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
        appliable_process_ir_request,
    )
    from _process_ir_capability_witnesses import _connector_scope  # noqa: E402
    from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

    record = load_registry().operation_records[0]
    operation, connection = record.operation_identity, record.connection_identity
    captures = (Path(__file__).resolve().parents[1]
                / "docs/architecture/evidence/issue-155/captures"
                / "cap155-e7-patch-operation-record")
    operation_xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")

    def _get_xml(_client, component_id, *_a, **_k):
        is_operation = component_id == operation.component_id
        return {
            "component_id": component_id,
            "version": operation.version if is_operation else connection.version,
            "xml": operation_xml if is_operation else connection_xml,
        }

    projections = []
    real = CR.project_grants_for_root

    def _spy(ir, symbols, *, process_root_ref, registry=None, snapshot=None):
        table = real(ir, symbols, process_root_ref=process_root_ref,
                     registry=registry, snapshot=snapshot)
        projections.append((
            [c.ref for c in table.idempotency_contracts],
            len(table.idempotency_grants),
        ))
        return table

    # The DECLARED config must agree with what the account stores — slice C's
    # comparison refuses a request declaring a GET while reusing a PATCH, and it
    # correctly refused an earlier version of this fixture.
    request = appliable_process_ir_request(
        units=(ProcessAuthoringUnitV1(
            envelope=ProcessComponentEnvelopeV1(
                component_key="proc", name="M12.15 Process", action="create",
                depends_on=("conn", "op", "getop")),
            process_ir=parse_process_ir_v1(_connector_scope(
                protected="$ref:op",
                upstream="$ref:getop",
                retry={"count": 2},
                idempotency={"kind": "key_reference",
                             "contract_ref": record.contract_ref},
            ))),),
        components=(
            dict(APPLIABLE_CONN, component_id=connection.component_id,
                 action="create",
                 config=dict(APPLIABLE_CONN["config"], reference_only=True)),
            dict(APPLIABLE_OP, component_id=operation.component_id, action="create",
                 config=dict(APPLIABLE_OP["config"], reference_only=True,
                             method="PATCH",
                             path="/admin/cdscm/api/v1/clients/"
                                  "41172938-b9bd-4495-b411-9851f5ac7b00")),
            dict(APPLIABLE_OP, key="getop", name="M12.15 get", action="create",
                 config=dict(APPLIABLE_OP["config"], component_name="M12.15 get")),
        ),
    )

    with patch.object(CR, "project_grants_for_root", _spy), \
         patch("boomi_mcp.categories.components._shared.component_get_xml", _get_xml), \
         patch("boomi_mcp.categories.integration_builder.paginate_metadata",
               lambda *a, **k: []):
        result, _internals = compile_authoring_request_v1(
            request, boomi_client=_evidenced_client(), profile="qa")

    assert result is not None, "the evidenced retried write did not compile"
    # EVERY projection, not just the first. The materialization plan recompiles
    # the IR, and it was handed the contract-free table — so semantic validation
    # passed on evidence the plan build then could not see, and compile refused
    # for want of what it had just accepted.
    assert projections, "no projection ran; this test proves nothing"
    assert all(refs == [record.contract_ref] and grants == 1
               for refs, grants in projections), projections


def _evidenced_reference_only_request():
    """The archived PATCH record, authored as a caller can author it.

    Shared by the compile witness above and the wet-apply witness below so the
    two cannot silently drift onto different documents — the whole point of the
    apply witness is that it is the SAME request travelling further.
    """
    from boomi_mcp.connector_replay.registry import load_registry

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _m12_11_support import (  # noqa: E402
        APPLIABLE_CONN,
        APPLIABLE_OP,
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
        appliable_process_ir_request,
    )
    from _process_ir_capability_witnesses import _connector_scope  # noqa: E402
    from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

    record = load_registry().operation_records[0]
    operation, connection = record.operation_identity, record.connection_identity
    return record, operation, connection, appliable_process_ir_request(
        units=(ProcessAuthoringUnitV1(
            envelope=ProcessComponentEnvelopeV1(
                component_key="proc", name="M12.15 Process", action="create",
                depends_on=("conn", "op", "getop")),
            process_ir=parse_process_ir_v1(_connector_scope(
                protected="$ref:op",
                upstream="$ref:getop",
                retry={"count": 2},
                idempotency={"kind": "key_reference",
                             "contract_ref": record.contract_ref},
            ))),),
        components=(
            dict(APPLIABLE_CONN, component_id=connection.component_id,
                 action="create",
                 config=dict(APPLIABLE_CONN["config"], reference_only=True)),
            dict(APPLIABLE_OP, component_id=operation.component_id, action="create",
                 config=dict(APPLIABLE_OP["config"], reference_only=True,
                             method="PATCH",
                             path="/admin/cdscm/api/v1/clients/"
                                  "41172938-b9bd-4495-b411-9851f5ac7b00")),
            dict(APPLIABLE_OP, key="getop", name="M12.15 get", action="create",
                 config=dict(APPLIABLE_OP["config"], component_name="M12.15 get")),
        ),
    )


def test_the_evidenced_write_survives_the_wet_apply_route_not_only_compile():
    """`action="apply", dry_run=False` — the route that produces bytes.

    Compile succeeding said less than it looked like it said. The pre-write pass
    inside apply recomputes the plan through `_dry_emit_canonical_plan`, and
    that call was handed a FRESH symbol table rather than the projected one two
    lines above it, so the recompile could not see the grants the projection had
    just minted: compile returned a plan and the wet route refused the same
    document for missing evidence. Nothing internal disagreed — the layers were
    each self-consistent — which is exactly why this witness drives the public
    entry with `dry_run` false and lets the write happen against a faked
    network, instead of asserting on a helper.
    """
    import json
    from unittest.mock import MagicMock, patch

    from boomi_mcp.categories.integration_builder import build_integration_action

    record, operation, connection, request = _evidenced_reference_only_request()
    captures = (Path(__file__).resolve().parents[1]
                / "docs/architecture/evidence/issue-155/captures"
                / "cap155-e7-patch-operation-record")
    operation_xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")

    submitted: dict = {}

    def _get_xml(_client, component_id, *_a, **_k):
        if component_id == "process-cid-1" and "xml" in submitted:
            return {"type": "process", "xml": submitted["xml"]}
        is_operation = component_id == operation.component_id
        return {
            "component_id": component_id,
            "type": "connector-settings",
            "version": operation.version if is_operation else connection.version,
            "xml": operation_xml if is_operation else connection_xml,
        }

    def _create(_client, _profile, payload_in):
        submitted["xml"] = payload_in["xml"]
        return {"_success": True, "component_id": "process-cid-1"}

    created = {"n": 0}

    def _component(*_a, **_k):
        created["n"] += 1
        return {"_success": True, "component_id": "cid-%d" % created["n"]}

    with patch("boomi_mcp.categories.integration_builder.paginate_metadata",
               lambda *a, **k: []), \
         patch("boomi_mcp.categories.integration_builder._execute_component",
               _component), \
         patch("boomi_mcp.categories.integration_builder.create_component", _create), \
         patch("boomi_mcp.categories.integration_builder.component_get_xml", _get_xml), \
         patch("boomi_mcp.categories.components._shared.component_get_xml", _get_xml):
        # The caller's real two-step: compile, then apply BOUND to that compile.
        # A typed apply refuses an unbound request outright, so the compile is
        # part of the route under test rather than setup — and it runs inside
        # the same fakes, because a reference-only reuse reads the account.
        payload = request.model_dump(mode="json")
        compiled = build_integration_action(
            _evidenced_client(), "qa", "compile",
            config={"authoring_request": payload},
        )
        assert compiled.get("_success") is True, compiled
        binding = compiled["authoring_result"]["revision_binding"]
        payload["expected_capability_revision"] = binding["capability_revision"]
        payload["expected_compile_hash"] = binding["compile_hash"]

        result = build_integration_action(
            _evidenced_client(), "qa", "apply",
            config={"authoring_request": payload, "dry_run": False},
        )

    assert result.get("_success") is True, result
    # The pre-write refusal this witness exists for names itself.
    assert "IDEMPOTENCY_EVIDENCE_MISSING" not in json.dumps(result), result
    # And the write actually happened — a route that refused before the first
    # submission would also satisfy an assertion about the absence of an error.
    assert submitted.get("xml"), "no process component was submitted"
    assert record.contract_ref  # the document under apply carried the reference


def test_an_unreadable_live_component_refuses_rather_than_trusting_the_caller():
    """A fetched document nobody can read is not a licence to believe the caller.

    The issue-level architect gate found this by probe: a live document the outer
    parser ACCEPTS and the strict identity reader REJECTS was still appended as a
    live identity, every one of its fields unknown. The declared-versus-live
    comparison skips unknown fields — correctly, since a declaration that says
    nothing asserts nothing — so the caller's declaration stood unchallenged, and
    an operation the account stores as a PATCH could be declared a GET and
    compiled inside a retried region with no grant.

    The distinction the code was missing is between never having asked and having
    asked and got an answer nobody can read. The second is the account being
    consulted and failing to answer, which this module already refuses when the
    FETCH fails; it now refuses when the fetch succeeds and the bytes do not.
    """
    import pathlib
    from unittest.mock import patch

    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )
    from boomi_mcp.categories.integration_builder import build_integration_action
    from boomi_mcp.connector_replay.registry import load_registry
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _m12_11_support import (  # noqa: E402
        APPLIABLE_CONN,
        APPLIABLE_OP,
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
        appliable_process_ir_request,
    )
    from _process_ir_capability_witnesses import _connector_scope  # noqa: E402

    # ACCEPTED by the outer parser, REJECTED by the strict reader. Both halves
    # are asserted, because a document that fails both parsers would exercise the
    # long-standing fetch-failure refusal instead of the one under test.
    unreadable = ('<?xml version="1.0"?><!DOCTYPE r [<!ENTITY e "x">]>'
                  '<bns:Component xmlns:bns="http://api.platform.boomi.com/" id="i"/>')
    import xml.etree.ElementTree as ET
    ET.fromstring(unreadable)
    assert not live_identity_from_component_xml("k", unreadable).readable

    record = load_registry().operation_records[0]
    operation, connection = record.operation_identity, record.connection_identity
    captures = (Path(__file__).resolve().parents[1]
                / "docs/architecture/evidence/issue-155/captures"
                / "cap155-e7-patch-operation-record")
    connection_xml = (captures / "component_connection.xml").read_text(encoding="utf-8")

    # DECLARED GET over a component the account actually holds as a PATCH, inside
    # a retried region and with no evidence: exactly the shape the caller's
    # declaration must not be able to authorise on its own.
    request = appliable_process_ir_request(
        units=(ProcessAuthoringUnitV1(
            envelope=ProcessComponentEnvelopeV1(
                component_key="proc", name="P", action="create",
                depends_on=("conn", "op", "getop")),
            process_ir=parse_process_ir_v1(_connector_scope(
                protected="$ref:op", upstream="$ref:getop", retry={"count": 2}))),),
        components=(
            dict(APPLIABLE_CONN, component_id=connection.component_id, action="create",
                 config=dict(APPLIABLE_CONN["config"], reference_only=True)),
            dict(APPLIABLE_OP, component_id=operation.component_id, action="create",
                 config=dict(APPLIABLE_OP["config"], reference_only=True, method="GET")),
            dict(APPLIABLE_OP, key="getop", name="G",
                 config=dict(APPLIABLE_OP["config"], component_name="G")),
        ),
    )

    def _get_xml(_client, component_id, *_a, **_k):
        is_operation = component_id == operation.component_id
        return {
            "component_id": component_id,
            "type": "connector-settings",
            "version": operation.version if is_operation else connection.version,
            "xml": unreadable if is_operation else connection_xml,
        }

    with patch("boomi_mcp.categories.integration_builder.paginate_metadata",
               lambda *a, **k: []), \
         patch("boomi_mcp.categories.components._shared.component_get_xml", _get_xml), \
         patch("boomi_mcp.categories.integration_builder.component_get_xml", _get_xml):
        result = build_integration_action(
            _evidenced_client(), "qa", "compile",
            config={"authoring_request": request.model_dump(mode="json")})

    assert result.get("_success") is not True, (
        "a retried call over a component nobody could read compiled on the "
        "strength of the caller's own declaration: %s" % (sorted(result),)
    )


def test_a_minted_grant_names_its_root_and_its_connection():
    """The two facts the durable attestation keys on, recorded where they are known.

    The attestation contract sorts and deduplicates bindings by
    `(process_root_ref, call_identity, contract_ref, operation_ref,
    connection_ref)`. Two of those five were unavailable to it — the grant
    carried neither — so the key it enforced was a three-field prefix and the
    dedup did not exist at all.

    Both are read from the minter's own inputs: the root it was called for, and
    the connection the compiler's binding resolution already attached to the
    operation. Asserting them HERE, at the mint, rather than at the attestation,
    because the attestation can only carry what the mint recorded.
    """
    table = project_grants_for_root(
        _doc({"kind": "key_reference", "contract_ref": _CONTRACT}),
        _symbols(),
        process_root_ref="$ref:ROOT",
        registry=_Registry(_record()),
        snapshot=_snapshot(),
    )
    assert table.idempotency_grants, "no grant was minted, so nothing is asserted"
    grant = table.idempotency_grants[0]
    assert grant.process_root_ref == "$ref:ROOT", grant
    # NOT a literal: the connection the compiler resolved for this call is the
    # only correct answer, and hard-coding a name here would pass even if the
    # minter attached some other connection's reference.
    from boomi_mcp.compiler.process_ir.connector_resolution import (
        resolve_connector_call_bindings,
    )
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg

    symbols = _symbols()
    cfg = lower_process_ir_to_cfg(
        _doc({"kind": "key_reference", "contract_ref": _CONTRACT})
    )
    resolved = {
        b.source_path: b.connection_ref
        for b in resolve_connector_call_bindings(cfg, symbols)
    }
    assert grant.connection_ref == resolved[grant.call_source_path], (
        grant, resolved,
    )


def test_the_relocatable_compile_cannot_be_the_evidence_gate():
    """WITHDRAWN REFUTATION, replaced by the behaviour it wrongly defended.

    THE NAME IS NOW WRONG AND IS KEPT ANYWAY, which is worth explaining because
    it looks like carelessness. Test identities are rows in an append-only,
    positionally-contiguous manifest: a row cannot be deleted without renumbering
    every row after it, and a row that is born and retired inside one
    baseline-to-head range cannot be tombstoned either — the wave gate refuses a
    tombstone for an identity that never existed at the base. This test was added
    inside this very range, so renaming it manufactures exactly that
    unexpressible row. The name stays; the docstring below is where the meaning
    lives, and a later slice whose baseline includes this test can rename it
    freely.

    This test previously argued that the materialization compile could not judge
    per-call evidence, and it argued that from a table built by DELETING the
    grants from a projected one — then described the result as what production
    supplies. The architect gate traced the real caller: the typed path projects
    a root and mints its grants, and hands THAT table to materialization, which
    was clearing both. So the clearing switched the requirement off on exactly
    the compile that produces the bytes, and my measurement had manufactured the
    state that made it look unavoidable.

    The isolation the clearing provided is kept at its own level: a grant records
    the root it was minted for, and the index admits only this root's own.
    """
    from boomi_mcp.authoring.process_materialization import placeholder_backed_symbols
    from boomi_mcp.compiler.process_ir.semantic_validation.pipeline import (
        validate_process_ir,
    )

    doc = _doc({"kind": "key_reference", "contract_ref": _CONTRACT})

    projected = project_grants_for_root(
        doc, _symbols(), process_root_ref="$ref:ROOT",
        registry=_Registry(_record()), snapshot=_snapshot(),
    )
    assert len(projected.idempotency_grants) == 1
    assert not validate_process_ir(doc, projected).errors

    # THE RELOCATABLE TABLE KEEPS BOTH, so the compile that emits the bytes is
    # held to the same rule as the one that wrote the report.
    relocatable = placeholder_backed_symbols(projected)
    assert relocatable.process_root_ref == "$ref:ROOT"
    assert len(relocatable.idempotency_grants) == 1
    assert not validate_process_ir(doc, relocatable).errors

    # ...and the requirement is genuinely armed there: drop the grant and the
    # same relocatable table refuses, which is what the clearing had prevented.
    grantless = relocatable.model_copy(update={"idempotency_grants": ()})
    assert "PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING" in [
        e.code for e in validate_process_ir(doc, grantless).errors
    ]

def test_one_process_root_can_mint_two_grants():
    """The recorded one-binding ceiling was FALSE, and this is the shape that
    disproves it.

    A live-QA round enumerated the structural rules and concluded the surface
    admits one evidenced connector call per process root, so the attestation's
    sort and deduplicate key could never be exercised. That enumeration covered
    the connector scope and missed the process scope: it constrains only the
    FIRST step of its try body — which must be the call that produces the
    documents — and says nothing about what follows. As the sole root step, with a
    producer first, it accepts further evidenced calls and the minter visits every
    one of them.

    Two grants in one root means two bindings in ONE attestation list, so the key
    is publicly reachable and the limit recorded against it is void.
    """
    import copy

    from boomi_mcp.models.process_ir import ProcessIRV1

    body = _doc({"kind": "key_reference", "contract_ref": _CONTRACT}).model_dump(mode="json")
    root = body["body"]["steps"]
    try_catch = [s for s in root if s.get("kind") == "try_catch"][0]
    producer = [s for s in root if s.get("kind") == "connector_call"]
    write = try_catch["try_body"]["steps"][0]

    try_catch["scope"] = "process"
    try_catch["retry"] = {"count": 2, "source_replay_policy": "allow_duplicates"}
    try_catch["try_body"]["steps"] = (
        ([copy.deepcopy(producer[0])] if producer else [])
        + [copy.deepcopy(write), copy.deepcopy(write)]
    )
    body["body"]["steps"] = [try_catch]

    table = project_grants_for_root(
        ProcessIRV1.model_validate(body), _symbols(), process_root_ref="$ref:ROOT",
        registry=_Registry(_record()), snapshot=_snapshot(),
    )
    assert len(table.idempotency_grants) == 2, [
        g.call_source_path for g in table.idempotency_grants
    ]
    # DISTINCT call sites, so the sort key has something to order.
    assert len({g.call_source_path for g in table.idempotency_grants}) == 2
