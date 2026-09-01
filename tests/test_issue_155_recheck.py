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
    def __init__(self, digest, op, conn, scope="a" * 64):
        self.record_digest = digest
        self.operation_identity = op
        self.connection_identity = conn
        self.account_scope_hash = scope


class _Registry:
    def __init__(self, records):
        self.operation_records = tuple(records)


class _Grant:
    def __init__(self, digest, contract_ref="$ref:C"):
        self.record_digest = digest
        self.contract_ref = contract_ref
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
    return lambda component_id, _kind: table.get(component_id)


def test_a_root_with_no_grant_reads_nothing_at_all():
    """The ordinary path pays nothing, and this is measured rather than asserted.

    A blanket re-read would make every apply depend on extra platform reads whose
    failure could only refuse work the evidence channel was never part of. The
    reader here counts its calls, so a future change that starts reading for
    grant-less roots fails this test instead of showing up as a platform-call
    regression nobody attributes.
    """
    calls = []

    def counting(component_id, _kind):
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
        live_identity=lambda component_id, _kind: None,
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

    def counting(component_id, kind):
        calls.append(component_id)
        return _live()(component_id, kind)

    outcome = recheck_grant_identities(
        grants=(_Grant(DIGEST), _Grant(DIGEST, contract_ref="$ref:C2")),
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
