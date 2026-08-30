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


def test_every_served_copy_of_the_action_list_names_every_router_action():
    """A docstring cannot be computed at import, so it is PINNED instead.

    Live QA found three served copies still reading the pre-slice list after only
    one had been derived: the capability catalogue's `actions`, its parameter
    prose, and the MCP tool description itself. A caller reading any of them is
    told the new action does not exist.
    """
    import pathlib

    from boomi_mcp.categories.components.query_components import QUERY_COMPONENTS_ACTIONS

    server_text = (
        pathlib.Path(__file__).resolve().parents[1] / "server.py"
    ).read_text()

    # The tool description a caller actually reads — scoped to THIS tool. Several
    # tools carry an "action: One of:" line, and taking the first match reads
    # another tool's contract, which is its own way of proving nothing.
    start = server_text.index("    def query_components(")
    end = server_text.index("\n    @", start)
    body = server_text[start:end]
    line = next(ln for ln in body.splitlines() if "action: One of:" in ln)
    for action in QUERY_COMPONENTS_ACTIONS:
        assert action in line, f"the served tool description omits {action!r}: {line.strip()}"


def test_the_capability_catalogue_serves_the_router_actions():
    """Reads the CATALOGUE, not the helper that feeds it.

    The first version of this asserted `_query_components_actions() ==
    QUERY_COMPONENTS_ACTIONS`, which is the same object compared to itself: it
    would stay green with the catalogue still serving the pre-slice list, which
    is exactly the state live QA found.
    """
    from boomi_mcp.categories.components.query_components import QUERY_COMPONENTS_ACTIONS
    from boomi_mcp.categories.meta_tools import list_capabilities_action

    catalogue = list_capabilities_action()
    served = catalogue["tools"]["query_components"]
    assert tuple(served["actions"]) == QUERY_COMPONENTS_ACTIONS, served["actions"]
    prose = served["parameters"]["action"]
    for action in QUERY_COMPONENTS_ACTIONS:
        assert action in prose, f"the served parameter prose omits {action!r}: {prose}"


def test_discovery_is_reachable_THROUGH_THE_ROUTER_not_only_as_a_function():
    """The test that was missing, and its absence let the capability ship broken.

    Live QA found every boundary call refused with "operation_component_id is
    required" over two readable ids. Two layers were wrong — the server built no
    `config` for this action, and the identity reader took `version` off the
    response envelope instead of off the fetched component — and NEITHER is
    traversed by a test that calls the module function with a hand-written
    closure. A capability is reachable or it is not, and only the boundary knows.
    """
    from boomi_mcp.categories.components import query_components as qc

    fetched = {}

    class _Client:
        pass

    def _fake_get_component(client, profile, component_id):
        # The REAL shape of this function's return value, which is where the
        # second defect lived: the version is on the component, not the envelope.
        fetched[component_id] = True
        return {
            "_success": True,
            "profile": profile,
            "component": {"id": component_id, "version": 7, "name": "n"},
        }

    original = qc.get_component
    qc.get_component = _fake_get_component
    try:
        result = qc.query_components_action(
            _Client(),
            "renera",
            "idempotency_contract_candidates",
            config={
                "operation_component_id": "op-1",
                "connection_component_id": "cn-1",
            },
        )
    finally:
        qc.get_component = original

    assert result["_success"] is True, result
    assert result["candidates"] == []
    assert result["authority"] == "non_authoritative_candidate"
    # The versions came from the fetched COMPONENT, so the reader traversed the
    # real shape rather than an envelope key that does not exist.
    assert result["operation_version"] == 7
    assert result["connection_version"] == 7
    assert fetched == {"op-1": True, "cn-1": True}


def test_every_router_action_receives_its_config_AT_THE_SERVER_BOUNDARY():
    """Derived from the action list, and driven at the layer that was broken.

    The first version of this grepped `server.py` for one literal `elif` line —
    a guard whose universe was the single action I had just added. Live QA
    defeated it in one move: a sixth action with a router arm but no server arm
    reproduces the original defect with every test still green. And the test that
    claimed to prove reachability called the ROUTER, one layer below the layer
    that was broken, while its own docstring said only the boundary knows.

    This drives `server.query_components` itself, once per action the router
    declares, and asserts the config the caller passed actually arrives. A new
    action without a params arm fails here without anyone remembering to add a
    case.
    """
    import json

    import server as server_module
    from boomi_mcp.categories.components import query_components as qc

    seen = {}

    def _spy(sdk, profile, action, **params):
        seen[action] = params
        return {"_success": True, "spy": True}

    original_router = server_module.query_components_action
    original_qc = qc.query_components_action
    server_module.query_components_action = _spy
    qc.query_components_action = _spy
    try:
        for action in qc.QUERY_COMPONENTS_ACTIONS:
            seen.clear()
            server_module.query_components(
                profile="renera",
                action=action,
                config=json.dumps({"probe_key": "probe_value"}),
            )
            assert action in seen, f"{action}: the server never reached the router"
            # The invariant is that EVERY action has a params arm — not that the
            # raw config is forwarded, since most actions legitimately extract a
            # specific key from it. An action with no arm produces exactly `{}`,
            # which is the shape the original defect had.
            assert seen[action] != {}, (
                f"{action}: the server built NO parameters for this action, so "
                f"whatever the caller sent in `config` is lost. A new action "
                f"needs its own arm in the server's parameter builder."
            )
    finally:
        server_module.query_components_action = original_router
        qc.query_components_action = original_qc


@pytest.mark.parametrize(
    "value", ["$ref:ABC\n", "$ref:ABC\r\n", "$ref:ABC\nX", "$ref:ABC\n$ref:D"]
)
def test_a_trailing_newline_does_not_slip_past_the_grammar(value):
    """`$` matches before a trailing newline, so `match` accepted a value the
    served JSON-Schema pattern declares invalid — the enforced rule and the
    published one disagreeing about the same string.

    Found by live QA as a REGRESSION against the pre-slice behaviour, which
    refused it. This module's own docstring documents the same trap for its other
    identifier grammars a hundred lines above the constant that reintroduced it.
    """
    import re

    from boomi_mcp.connector_replay.ids import (
        AUTHORED_CONTRACT_REF_PATTERN,
        is_authored_contract_ref,
    )

    assert is_authored_contract_ref(value) is False

    # ...and the enforced answer agrees with the SERVED pattern, which is what a
    # machine validating against the schema would compute.
    assert re.fullmatch(AUTHORED_CONTRACT_REF_PATTERN, value) is None


def test_changing_HOW_the_grammar_matches_moves_the_revision():
    """Live QA: `.match` → `.fullmatch` changed what the server accepts and NO
    revision moved, because the pattern STRING had not changed.

    A revision that stands still while behaviour moves is the failure the
    manifest exists to detect, so the fingerprint covers the grammar's verdicts
    on a fixed probe vocabulary rather than only its pattern text.
    """
    from boomi_mcp.authoring import contract as contract_module
    from boomi_mcp.connector_replay import ids

    baseline = contract_module._compiler_revision()

    class _ShimIds:
        AUTHORED_CONTRACT_REF_PROBES = ids.AUTHORED_CONTRACT_REF_PROBES

        @staticmethod
        def authored_contract_ref_behaviour():
            # Exactly what a `match`-instead-of-fullmatch implementation answers:
            # the trailing-newline probes flip to accepted.
            return tuple(
                (probe, True if probe.rstrip("\r\n") != probe else verdict)
                for probe, verdict in ids.authored_contract_ref_behaviour()
            )

    original = contract_module._replay_ids
    contract_module._replay_ids = lambda: _ShimIds
    try:
        moved = contract_module._compiler_revision()
    finally:
        contract_module._replay_ids = original

    assert moved != baseline, (
        "a change to how the grammar matches left the revision unchanged; "
        "two deployments differing in what they ACCEPT would report no drift"
    )
    assert contract_module._compiler_revision() == baseline, "shim leaked"


@pytest.mark.parametrize("deleted", [True, "true", "TRUE"])
def test_a_soft_deleted_component_is_not_a_live_identity(deleted):
    """Live QA: the account still serves a deleted component WITH a version, so
    reading the version alone reported it as the thing a candidate matches."""
    from boomi_mcp.categories.components import query_components as qc

    def _fake_get_component(client, profile, component_id):
        return {
            "_success": True,
            "profile": profile,
            "component": {"id": component_id, "version": 3, "deleted": deleted},
        }

    original = qc.get_component
    qc.get_component = _fake_get_component
    try:
        result = qc.query_components_action(
            object(), "renera", "idempotency_contract_candidates",
            config={"operation_component_id": "op-1", "connection_component_id": "cn-1"},
        )
    finally:
        qc.get_component = original

    from boomi_mcp.errors import CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE

    assert result["_success"] is False
    assert result["error_code"] == CONNECTOR_REPLAY_DISCOVERY_IDENTITY_UNAVAILABLE


def test_a_live_component_still_resolves():
    """Control: the deleted check must not refuse ordinary components."""
    from boomi_mcp.categories.components import query_components as qc

    def _fake_get_component(client, profile, component_id):
        return {"_success": True, "profile": profile,
                "component": {"id": component_id, "version": 5, "deleted": "false"}}

    original = qc.get_component
    qc.get_component = _fake_get_component
    try:
        result = qc.query_components_action(
            object(), "renera", "idempotency_contract_candidates",
            config={"operation_component_id": "op-1", "connection_component_id": "cn-1"},
        )
    finally:
        qc.get_component = original
    assert result["_success"] is True
    assert result["operation_version"] == 5
