"""#155 slice D — idempotency-contract candidate discovery."""

import pytest

from boomi_mcp.connector_replay.discovery import (
    CANDIDATE_AUTHORITY,
    CANDIDATE_FIELDS,
    idempotency_contract_candidates,
)


def _live(version=3):
    return lambda component_id: {"component_id": component_id, "version": version}


class _Registry:
    def __init__(self, records=()):
        self.operation_records = tuple(records)


def test_no_record_for_the_pair_is_an_answer_not_a_failure():
    """An empty list is the normal state until evidence is ingested. Reporting it
    as an error would make the absence of evidence look like a broken read."""
    result = idempotency_contract_candidates(
        operation_component_id="op-1",
        connection_component_id="cn-1",
        live_identity=_live(),
        registry=_Registry(),
    )
    assert result["_success"] is True
    assert result["candidates"] == []
    assert result["authority"] == CANDIDATE_AUTHORITY


@pytest.mark.parametrize("unreadable", ["op-1", "cn-1"])
def test_an_unreadable_identity_fails_closed_and_names_which_side(unreadable):
    """"Unavailable" with no subject makes the caller guess which of two
    components to investigate."""
    from boomi_mcp.errors import CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE

    def live(component_id):
        return None if component_id == unreadable else {"component_id": component_id, "version": 1}

    result = idempotency_contract_candidates(
        operation_component_id="op-1",
        connection_component_id="cn-1",
        live_identity=live,
        registry=_Registry(),
    )
    assert result["_success"] is False
    assert result["error_code"] == CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE
    assert unreadable in result["error"]


@pytest.mark.parametrize("blank", ["", "   ", None, 7])
def test_a_missing_component_id_is_refused_by_the_same_code(blank):
    from boomi_mcp.errors import CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE

    result = idempotency_contract_candidates(
        operation_component_id=blank,
        connection_component_id="cn-1",
        live_identity=_live(),
        registry=_Registry(),
    )
    assert result["_success"] is False
    assert result["error_code"] == CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE


def test_the_served_candidate_field_set_is_CLOSED():
    """The projection names what it emits, so nothing can ride along.

    Asserted as EQUALITY against the declared set, not containment: a projection
    that grows a field silently is how a closed contract stops being closed. The
    registry record carries a capture reference and a route coverage that names a
    path; neither may appear.
    """
    from boomi_mcp.connector_replay.discovery import _candidate

    class _Identity:
        def __init__(self, cid, version):
            self.component_id, self.version = cid, version

    class _Coverage:
        route = "/admin/api/v1/clients/{id}"      # a path — must NOT be served

    class _Record:
        contract_ref = "$ref:C"
        family = "rest"
        action = "PATCH"
        semantics_id = "icv1"
        semantics_revision = 1
        account_scope_hash = "a" * 64
        operation_identity = _Identity("op-1", 4)
        connection_identity = _Identity("cn-1", 2)
        route_coverage = _Coverage()
        record_digest = "b" * 64
        capture = {"raw_body": "SHOULD-NEVER-BE-SERVED"}

    served = _candidate(_Record())
    assert set(served) == set(CANDIDATE_FIELDS)

    blob = repr(served)
    assert "SHOULD-NEVER-BE-SERVED" not in blob
    assert "/admin/api/v1/clients" not in blob, "a route path reached the projection"
    assert served["route_coverage_kind"] == "_Coverage"


def test_the_served_action_list_is_derived_from_the_router():
    """A hand-copied action list is how the catalogue came to advertise ten
    monitoring actions where the router accepted seventeen."""
    from boomi_mcp.categories.components.query_components import QUERY_COMPONENTS_ACTIONS
    from boomi_mcp.categories.meta_tools import _query_components_actions

    assert tuple(_query_components_actions()) == QUERY_COMPONENTS_ACTIONS
    assert "idempotency_contract_candidates" in QUERY_COMPONENTS_ACTIONS


def test_the_revision_moves_on_class_semantics_and_not_on_an_account_record(monkeypatch):
    """The fingerprint's account-INDEPENDENCE, asserted in both directions.

    Class-level replay semantics are part of what a document is compiled against,
    so the revision must move when they change. An operation record is scoped to
    ONE account — it carries an account scope hash — so a revision that moved
    when an account minted a record would report drift between two deployments of
    byte-identical code, which is the failure this revision exists not to have.

    Both halves matter: a witness that only proved movement would pass just as
    well if the loader read the whole registry.
    """
    from boomi_mcp.authoring import contract as contract_module

    baseline = contract_module._compiler_revision()

    class _Semantics:
        def model_dump(self, mode="json"):
            return {"semantics_id": "icv1", "revision": 2}

    class _Registry:
        def __init__(self, semantics=(), records=()):
            self.semantics_definitions = tuple(semantics)
            self.operation_records = tuple(records)

    # A class-level semantics change MOVES the revision.
    monkeypatch.setattr(
        contract_module, "_replay_registry", lambda: _Registry(semantics=[_Semantics()])
    )
    with_semantics = contract_module._compiler_revision()
    assert with_semantics != baseline, "a class-level semantics change did not move it"

    # An account-scoped operation record does NOT.
    class _Record:
        account_scope_hash = "c" * 64

    monkeypatch.setattr(
        contract_module, "_replay_registry", lambda: _Registry(records=[_Record()])
    )
    with_record = contract_module._compiler_revision()
    assert with_record == baseline, (
        "minting an account-scoped operation record moved the relocatable "
        "fingerprint; two deployments of identical code would report drift"
    )
