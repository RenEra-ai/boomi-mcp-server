"""Slice E — the apply-boundary recheck, driven as a unit and at the boundary.

The unit tests here cover the comparison itself. The tests that matter more live
beside them and drive the PUBLIC apply entry: #180's lesson is that three internal
layers agreeing about a channel says nothing about whether the channel reaches
apply, and slice D's own review found the grant gate wired-but-inert twice.
"""

from __future__ import annotations

import pytest

from boomi_mcp.connector_replay.recheck import (
    DRIFT_REASONS,
    recheck_grant_identities,
)


class _Identity:
    def __init__(self, component_id, version, config_digest):
        self.component_id = component_id
        self.version = version
        self.config_digest = config_digest


class _Record:
    # `contract_ref` and `route_coverage` are NOT optional on the real model
    # (`OperationContractRecordV1`), and leaving them off a stand-in is how a
    # fixture stops being able to exhibit what the code now checks: the recheck
    # resolves a grant by the (digest, contract_ref) PAIR, because the loader
    # dedupes contract refs and not digests.
    def __init__(self, digest, op, conn, scope="a" * 64, contract_ref="$ref:C",
                 route_coverage=None):
        self.record_digest = digest
        self.contract_ref = contract_ref
        self.operation_identity = op
        self.connection_identity = conn
        self.account_scope_hash = scope
        self.route_coverage = route_coverage


class _Registry:
    def __init__(self, records):
        self.operation_records = tuple(records)


class _Grant:
    def __init__(self, digest, contract_ref="$ref:C", dynamic_path=False,
                 route_digest=None):
        self.record_digest = digest
        self.contract_ref = contract_ref
        self.dynamic_path = dynamic_path
        self.route_digest = route_digest
        self.operation_ref = "$ref:op"
        self.call_source_path = "/body/steps/0"


DIGEST = "b" * 64
OP_CFG = "ComponentConfigDigestV1:" + "1" * 64
CONN_CFG = "ComponentConfigDigestV1:" + "2" * 64


def _registry():
    return _Registry([
        _Record(
            DIGEST,
            _Identity("op-1", 3, OP_CFG),
            _Identity("conn-1", 5, CONN_CFG),
        )
    ])


def _live(**overrides):
    table = {
        "op-1": {"version": 3, "config_digest": OP_CFG},
        "conn-1": {"version": 5, "config_digest": CONN_CFG},
    }
    table.update(overrides)
    return lambda component_id, _kind, _family=None: table.get(component_id)


def test_a_root_with_no_grant_reads_nothing_at_all():
    """The ordinary path pays nothing, and this is measured rather than asserted.

    A blanket re-read would make every apply depend on extra platform reads whose
    failure could only refuse work the evidence channel was never part of. The
    reader here counts its calls, so a future change that starts reading for
    grant-less roots fails this test instead of showing up as a platform-call
    regression nobody attributes.
    """
    calls = []

    def counting(component_id, _kind, _family=None):
        calls.append(component_id)
        return {"version": 1, "config_digest": OP_CFG}

    outcome = recheck_grant_identities(
        grants=(), registry=_registry(), live_identity=counting
    )
    assert outcome.ok
    assert calls == []


def test_the_matching_account_passes():
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),),
        registry=_registry(),
        live_identity=_live(),
        account_scope_hash="a" * 64,
    )
    assert outcome.ok, (outcome.drifts, outcome.unavailable)


@pytest.mark.parametrize(
    "override,expected",
    [
        ({"op-1": {"version": 4, "config_digest": OP_CFG}}, "operation_version"),
        (
            {"op-1": {"version": 3, "config_digest": "ComponentConfigDigestV1:" + "9" * 64}},
            "operation_config_digest",
        ),
        ({"conn-1": {"version": 6, "config_digest": CONN_CFG}}, "connection_version"),
        (
            {"conn-1": {"version": 5, "config_digest": "ComponentConfigDigestV1:" + "9" * 64}},
            "connection_config_digest",
        ),
    ],
)
def test_each_half_of_each_identity_drifts_on_its_own(override, expected):
    """Four arms, because a credential-only edit moves the version alone.

    Checking only one half would pass exactly the case the other half exists for:
    a version that advanced without a behaviour change, or a configuration that
    changed under a version that did not move.
    """
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),),
        registry=_registry(),
        live_identity=_live(**override),
    )
    assert not outcome.ok
    assert [d["reason"] for d in outcome.drifts] == [expected]
    assert expected in DRIFT_REASONS


def test_an_unreadable_component_is_a_refusal_and_not_a_pass():
    """Silence is the fail-open this whole channel exists to remove."""
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),),
        registry=_registry(),
        live_identity=lambda component_id, _kind, _family=None: None,
    )
    assert not outcome.ok
    assert outcome.unavailable["subject"] == "operation"
    assert outcome.unavailable["component_id"] == "op-1"
    assert not outcome.drifts


def test_a_grant_whose_record_is_gone_is_unavailable_not_clean():
    """"No drift" and "nothing left to compare against" are different answers."""
    outcome = recheck_grant_identities(
        grants=(_Grant("c" * 64),),
        registry=_registry(),
        live_identity=_live(),
    )
    assert not outcome.ok
    assert outcome.unavailable["subject"] == "operation_record"


def test_a_foreign_account_scope_drifts():
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),),
        registry=_registry(),
        live_identity=_live(),
        account_scope_hash="f" * 64,
    )
    assert [d["reason"] for d in outcome.drifts] == ["account_scope"]


def test_no_digest_and_no_scope_hash_reaches_the_caller():
    """The refusal travels; the projection it is built from must not.

    A config digest is computed over a projection of the component and a scope
    hash identifies an account. Neither is served, and this asserts over the whole
    rendered payload rather than over the fields it happens to inspect.
    """
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),),
        registry=_registry(),
        live_identity=_live(**{"op-1": {"version": 9, "config_digest": "ComponentConfigDigestV1:" + "9" * 64}}),
        account_scope_hash="f" * 64,
    )
    rendered = repr(outcome.drifts)
    assert "ComponentConfigDigestV1" not in rendered
    assert "9" * 64 not in rendered
    assert "f" * 64 not in rendered
    assert "a" * 64 not in rendered


def test_two_grants_over_one_component_read_it_once():
    """Two reads could return two answers inside one recheck, which is not a check."""
    calls = []

    def counting(component_id, kind, family=None):
        calls.append(component_id)
        return _live()(component_id, kind, family)

    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST), _Grant(DIGEST)),
        registry=_registry(),
        live_identity=counting,
    )
    assert outcome.ok
    assert sorted(calls) == ["conn-1", "op-1"]


def test_the_boundary_travels_with_the_outcome():
    """Pre and post are not the same refusal, so the outcome carries which it is."""
    for boundary in ("pre_submission", "post_submission"):
        outcome = recheck_grant_identities(
            grants=(_Grant(DIGEST),),
            registry=_registry(),
            live_identity=_live(),
            boundary=boundary,
        )
        assert outcome.boundary == boundary


def test_a_reason_shape_is_never_mistaken_for_an_identity():
    """The reader's return contract changed, so pin what a consumer must handle.

    `live_identity` used to return an identity or `None`. It now returns an
    identity or `{"reason": …}`, because collapsing every failure into `None` let
    the comparison render a server-side projection gap as "the account's identity
    could not be read" — false for that cause, and its remediation cannot fix it.
    Live QA measured the gap on 30 of 45 of one account's connector components.

    A reason mapping is TRUTHY, which is exactly how a careless consumer would
    read it as an identity with a missing version. Both shapes are driven here so
    that reading cannot creep back in, and the two sentences are asserted apart —
    they name different systems, and a remediation aimed at the wrong one is worse
    than none.
    """
    for reason, names_the_account in (
        ("account_unreadable", True),
        ("projection_unsupported", False),
    ):
        outcome = recheck_grant_identities(
            grants=(_Grant(DIGEST),),
            registry=_registry(),
            live_identity=lambda component_id, _kind, _family=None: {"reason": reason},
        )
        assert not outcome.ok, reason
        assert not outcome.drifts, "a reason shape was compared as if it were an identity"
        assert outcome.unavailable["reason"] == reason
        detail = outcome.unavailable["detail"]
        assert ("could not be read" in detail) is names_the_account, detail
        if not names_the_account:
            assert "the account is not at fault" in detail


def test_the_record_s_family_reaches_the_projection():
    """The evidence record carries a family; the reader must not default past it.

    The projection is per-family and defaults to REST, so digesting a database
    component under the REST projection compares two different things and then
    reports the account as unreadable. The record carries the family precisely so
    it need not be guessed, and this asserts it arrives.
    """
    seen = []

    def reader(component_id, kind, family=None):
        seen.append((component_id, kind, family))
        return _live()(component_id, kind, family)

    class _FamiliedRegistry:
        operation_records = tuple(
            type("R", (), {**{k: getattr(r, k) for k in
                             ("record_digest", "contract_ref", "route_coverage",
                              "operation_identity", "connection_identity",
                              "account_scope_hash")},
                           "family": "database"})()
            for r in _registry().operation_records
        )

    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),), registry=_FamiliedRegistry(), live_identity=reader
    )
    assert outcome.ok, (outcome.drifts, outcome.unavailable)
    assert seen, "the reader was never called"
    assert {f for _c, _k, f in seen} == {"database"}, seen


def test_one_component_read_under_two_families_is_two_readings():
    """The cache key must follow everything the reading depends on.

    It keyed on the component id alone, which was right while the reading
    depended on nothing else — and stopped being right the moment the record's
    family reached the projection, because one component digests differently
    under two families. Two grants of different families over the same component
    then shared the first one's reading and a truthful record took a false digest
    drift. Each grant alone passes, so only a cross-family pair exposes it.
    """
    seen = []

    def reader(component_id, kind, family=None):
        seen.append((component_id, kind, family))
        # Each family legitimately projects to a different digest.
        return {"version": 3, "config_digest": f"ComponentConfigDigestV1:{family[0] * 64}"}

    class _Rec:
        def __init__(self, family, digest_char):
            self.record_digest = digest_char * 64
            self.contract_ref = "$ref:C" if digest_char == "1" else "$ref:C2"
            self.route_coverage = None
            self.family = family
            self.account_scope_hash = "a" * 64
            cfg = f"ComponentConfigDigestV1:{family[0] * 64}"
            self.operation_identity = _Identity("shared-op", 3, cfg)
            self.connection_identity = _Identity("shared-op", 3, cfg)

    class _Reg:
        operation_records = (_Rec("rest", "1"), _Rec("database", "2"))

    outcome = recheck_grant_identities(
        grants=(_Grant("1" * 64), _Grant("2" * 64, contract_ref="$ref:C2")),
        registry=_Reg(),
        live_identity=reader,
    )
    families = {f for _c, _k, f in seen}
    assert families == {"rest", "database"}, (
        "the two families shared one reading, so a truthful record would take a "
        "false digest drift: %r" % (seen,)
    )
    assert outcome.ok, (outcome.drifts, outcome.unavailable)


class _StaticCoverage:
    kind = "static_path"

    def __init__(self, *digests):
        self.route_digests = tuple(digests)


class _ServiceWideCoverage:
    kind = "service_wide"


ROUTE = "RouteDigestV1:" + "7" * 64


def test_a_dynamic_path_needs_service_wide_coverage():
    """The check that was never written, and the closed field set hid it.

    Every other comparison could match while the record covered a different route
    entirely — or covered enumerated static routes while the call composes its path
    per document. The models say it in as many words: a dynamically bound path has
    no static digest that identifies it, so the evidence must be service-wide or it
    does not exist. The compiler cannot check this because it has no live reading;
    the ledger assigns it to this boundary, and nothing here read it.
    """
    registry = _Registry([
        _Record(DIGEST, _Identity("op-1", 3, OP_CFG), _Identity("conn-1", 5, CONN_CFG),
                route_coverage=_StaticCoverage(ROUTE))
    ])
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST, dynamic_path=True),),
        registry=registry, live_identity=_live(),
    )
    assert [d["reason"] for d in outcome.drifts] == ["route_coverage"]

    # The control: the SAME dynamic call against service-wide coverage passes, so
    # the refusal is about coverage and not about dynamic paths as such.
    ok = recheck_grant_identities(
        grants=(_Grant(DIGEST, dynamic_path=True),),
        registry=_Registry([
            _Record(DIGEST, _Identity("op-1", 3, OP_CFG),
                    _Identity("conn-1", 5, CONN_CFG),
                    route_coverage=_ServiceWideCoverage())
        ]),
        live_identity=_live(),
    )
    assert ok.ok, (ok.drifts, ok.unavailable)


def test_a_static_record_authorises_only_the_routes_it_enumerates():
    """And the route is COMPUTED from the live bytes, not taken from the grant.

    A first version read a `route_digest` off the grant. The real
    `IdempotencyGrantSymbolV1` forbids extra fields and has no such field, and its
    minter supplies none — so every real static grant was refused as route drift,
    and the test passed only because the fake grant invented the field. That is
    the fixture-you-wrote-is-not-evidence failure in its purest form, and the
    correction is not to add the field to the fake but to compute the route where
    the live bytes are: this boundary already reads both components, and a route
    digest is a function of the connection and the operation together.
    """
    import pathlib as _pathlib

    from boomi_mcp.connector_replay.digests import route_digest_v1

    caps = (
        _pathlib.Path(__file__).resolve().parents[1]
        / "docs/architecture/evidence/issue-155/captures"
    )
    conn_xml = (caps / "cap155-e1-conn-readback" / "rest-conn-c4281346.xml").read_text()
    op_xml = sorted(caps.rglob("operation_component.xml"))[0].read_text()
    live_route = route_digest_v1(conn_xml, op_xml)

    def _reader(component_id, kind, family=None):
        xml = op_xml if kind == "operation" else conn_xml
        return {
            "version": 3 if kind == "operation" else 5,
            "config_digest": OP_CFG if kind == "operation" else CONN_CFG,
            "xml": xml,
        }

    def _registry_covering(*routes):
        return _Registry([
            _Record(DIGEST, _Identity("op-1", 3, OP_CFG),
                    _Identity("conn-1", 5, CONN_CFG),
                    route_coverage=_StaticCoverage(*routes))
        ])

    # COVERED: the record enumerates the route the live components actually form.
    covered = recheck_grant_identities(
        grants=(_Grant(DIGEST),), registry=_registry_covering(live_route),
        live_identity=_reader,
    )
    assert covered.ok, (covered.drifts, covered.unavailable)

    # NOT COVERED: a record enumerating some other route does not authorise this one.
    other = "RouteDigestV1:" + "8" * 64
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),), registry=_registry_covering(other),
        live_identity=_reader,
    )
    assert [d["reason"] for d in outcome.drifts] == ["route_coverage"]

    # UNCOMPUTABLE: a reader that returns no bytes cannot name the route, and an
    # unnameable route is not a covered one — the fail-open direction, closed.
    blind = recheck_grant_identities(
        grants=(_Grant(DIGEST),), registry=_registry_covering(live_route),
        live_identity=lambda c, k, f=None: {
            "version": 3 if k == "operation" else 5,
            "config_digest": OP_CFG if k == "operation" else CONN_CFG,
        },
    )
    assert [d["reason"] for d in blind.drifts] == ["route_coverage"]
    assert blind.drifts[0]["observed"] == "uncomputable"
    # And no route is ever served in the clear.
    assert live_route not in repr(outcome.drifts) + repr(blind.drifts)


def test_two_records_sharing_a_digest_and_contract_are_refused_not_resolved():
    """A grant names ONE record. Two candidates is not a tie to break silently.

    The loader dedupes contract refs and does not reject duplicate digests, and a
    dict comprehension over the digest keeps the last — so a grant minted for one
    contract could be rechecked against another record entirely, and matching
    identities on the wrong one would authorise a write the granted record no
    longer covers.
    """
    twin = _Record(DIGEST, _Identity("op-1", 3, OP_CFG), _Identity("conn-1", 5, CONN_CFG))
    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST),),
        registry=_Registry([twin, twin]),
        live_identity=_live(),
    )
    assert not outcome.ok
    assert outcome.unavailable["reason"] == "ambiguous_record"


def test_a_soft_deleted_component_is_not_a_live_component():
    """Boomi returns a deleted component with its original version and XML, and the
    configuration digest projects nothing that moves with deletion — so every
    comparison passes and the write proceeds against a component that is gone."""
    for label, fetched in (
        ("root attribute", {"xml": '<bns:Component deleted="true" version="3"/>',
                            "version": 3}),
        ("fetched metadata", {"xml": "<x/>", "version": 3, "deleted": True}),
    ):
        outcome = recheck_grant_identities(
            grants=(_Grant(DIGEST),),
            registry=_registry(),
            live_identity=lambda c, k, f=None, _f=fetched: (
                __import__("boomi_mcp.connector_replay.recheck", fromlist=["x"])
                .live_identity_reader(None, read_component_xml=lambda *a, **kw: _f)(c, k, f)
            ),
        )
        assert not outcome.ok, label
        assert outcome.unavailable["reason"] == "component_deleted", label
        assert "DELETED" in outcome.unavailable["detail"], label
