"""Slice C — the pre-apply, credential-free connector identity projection.

The projection answers one question: does this config settle a route, a family
and an action well enough to mint an identity from? Its whole value is in
answering NO when it does not, so most of these pin refusals.
"""
from __future__ import annotations

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    SET_BY_EXTENSION,
    normalized_identity_projection,
)


def test_a_static_rest_config_is_mintable():
    """The positive control. Without it every refusal below could be vacuous."""
    identity = normalized_identity_projection(
        {
            "connector_type": "rest_client",
            "method": "get",
            "base_url": "http://host.docker.internal:8081",
            "path": "/admin/cdscm/api/v1/clients",
        }
    )
    assert identity.family == "rest"
    assert identity.action == "GET"          # normalized, not echoed
    assert identity.route_state == "static"
    assert identity.mintable


def test_declared_path_replacements_make_the_route_dynamic_not_static():
    """``build()`` BLANKS the path when replacements are usable.

    Reading ``config['path']`` here would report a route the emitted operation
    provably does not have — the path arrives per document at the process step.
    """
    identity = normalized_identity_projection(
        {
            "connector_type": "rest_client",
            "method": "GET",
            "base_url": "http://host.docker.internal:8081",
            "path": "/admin/cdscm/api/v1/clients/{key}",
            "path_replacements": [{"name": "key", "target_path": "clientId"}],
        }
    )
    assert identity.route_state == "dynamic"
    assert not identity.mintable
    assert identity.path is None, "a template path must not be published as the route"


def test_a_malformed_replacement_declaration_mints_nothing():
    """FOUND BY PROBING, not by reading: the first version answered 'static'.

    ``validate_config`` refuses a present-but-unusable declaration outright, so
    no component is ever built from this config. Calling it a static route would
    mint an identity for a thing that cannot exist.
    """
    for bad in ([{}], "yes", [{"name": "k"}], [{"name": " ", "target_path": "c"}]):
        identity = normalized_identity_projection(
            {
                "connector_type": "rest_client",
                "method": "GET",
                "base_url": "http://host.docker.internal:8081",
                "path": "/x",
                "path_replacements": bad,
            }
        )
        assert identity.route_state == "unavailable", bad
        assert not identity.mintable, bad


def test_an_extension_bound_endpoint_mints_nothing():
    """The ACCOUNT decides the route, so nothing here can pin it."""
    identity = normalized_identity_projection(
        {"connector_type": "rest_client", "method": "GET", "base_url": SET_BY_EXTENSION}
    )
    assert identity.route_state == "unavailable"
    assert not identity.mintable


@pytest.mark.parametrize("method", [7, None, ["GET"], "TELEPORT"])
def test_an_action_the_family_does_not_support_is_not_derived(method):
    """A non-str or unknown method must not project as a valid-looking action."""
    identity = normalized_identity_projection(
        {"connector_type": "rest_client", "method": method, "base_url": "http://h:8081"}
    )
    assert identity.action is None
    assert not identity.mintable


def test_the_database_family_pins_its_route_with_discrete_fields():
    identity = normalized_identity_projection(
        {
            "connector_type": "database",
            "operation_mode": "get",
            "host": "host.docker.internal",
            "port": 11433,
            "dbname": "Expert",
        }
    )
    assert identity.family == "database"
    assert identity.action == "Get"
    assert identity.endpoint == "host.docker.internal:11433/Expert"
    assert identity.mintable


def test_a_route_nobody_read_is_not_a_route_anybody_knows():
    """The second fail-open the probe exposed: mintable with ``endpoint=None``."""
    identity = normalized_identity_projection(
        {"connector_type": "database", "operation_mode": "get"}
    )
    assert identity.endpoint is None
    assert not identity.mintable


def test_the_endpoint_is_reduced_to_a_credential_free_skeleton():
    """A URL PATH can carry a webhook token or a session id.

    Reduced by the same rule the reuse surface already applies. RECORDED
    LIMITATION, asserted so it cannot drift silently: two connections differing
    ONLY in base-URL path project identically here. Full-fidelity route identity
    is the applied component's digest, which reads XML after the fact.
    """
    secret = normalized_identity_projection(
        {"connector_type": "rest_client", "method": "GET",
         "base_url": "https://hook.example.com/t/SECRET-TOKEN-abc"}
    )
    assert secret.endpoint == "https://hook.example.com"
    assert "SECRET-TOKEN-abc" not in (secret.endpoint or "")

    other = normalized_identity_projection(
        {"connector_type": "rest_client", "method": "GET",
         "base_url": "https://hook.example.com/t/DIFFERENT"}
    )
    assert other.endpoint == secret.endpoint, "the recorded limitation changed"


def test_a_family_the_allowlist_does_not_admit_mints_nothing():
    """Plain ``http`` and the listener family are declined, deliberately."""
    identity = normalized_identity_projection(
        {"connector_type": "http", "method": "GET", "base_url": "http://h:8081"}
    )
    assert identity.family is None
    assert not identity.mintable


def test_a_non_mapping_config_refuses_rather_than_raising():
    """It runs on unvalidated input; raising would make it unusable as a probe."""
    for junk in (None, "config", 7, ["connector_type"]):
        identity = normalized_identity_projection(junk)
        assert identity.route_state == "unavailable"
        assert not identity.mintable


def test_the_projection_never_calls_build():
    """Structural: ``build()`` raises on any validation failure.

    A projection that raised could not report 'this config settles nothing',
    which is the answer it exists to give.

    Checked over the AST, not the source TEXT. The first version grepped for
    ``.build(`` and matched this function's own docstring, which says it does
    not call build — a scan that cannot tell prose from a call proves nothing
    about either.
    """
    import ast
    import inspect
    import textwrap

    from boomi_mcp.categories.components.builders import connector_builder

    tree = ast.parse(
        textwrap.dedent(inspect.getsource(connector_builder.normalized_identity_projection))
    )
    called = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "build" not in called, f"the projection calls build(): {sorted(called)}"

    # Non-vacuity: the same walk DOES see the calls it really makes.
    assert called, "the AST walk found no attribute calls at all — it proves nothing"


# --- slice C: the canonical symbol table has ONE construction --------------------


def test_the_canonical_symbol_table_is_built_in_exactly_one_place():
    """A second builder of an already-resolved fact is this slice's own defect class.

    The apply path hand-copied ``_build_canonical_symbols`` with byte-identical
    arguments, 280 lines below the helper — which is why that helper's docstring
    claimed three callers while only two used it. Threading anything new into the
    table therefore had two places to reach and would have reached one.

    Counted over the AST so a formatting change cannot silently satisfy it.
    """
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    module = root / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())

    # The ONE sanctioned construction lives inside the shared helper. An earlier
    # version of this guard forbade every direct call including that one, so it
    # failed on a clean tree — a guard that cannot pass is not a guard.
    sanctioned = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_build_canonical_symbols"
    )
    inside_helper = {
        id(node)
        for node in ast.walk(sanctioned)
        if isinstance(node, ast.Call)
    }
    direct = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "build_symbol_table"
        and id(node) not in inside_helper
    ]
    assert direct == [], (
        "integration_builder builds the symbol table outside the shared helper at "
        f"line(s) {[n.lineno for n in direct]} — route it through _build_canonical_symbols"
    )

    helper = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_build_canonical_symbols"
    ]
    assert len(helper) >= 3, (
        "the shared construction lost a caller; its docstring claims three "
        f"(plan, apply, pre-write dry emit), found {len(helper)}"
    )


# --- slice C: the identity comparison, and the REACH it does not yet have --------


def _identity(key, **kw):
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ResolvedConnectorComponentIdentityV1,
    )

    return ResolvedConnectorComponentIdentityV1(component_key=key, **kw)


def _snapshot(*identities):
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        TrustedConnectorResolutionSnapshotV1,
    )

    return TrustedConnectorResolutionSnapshotV1(identities=tuple(identities))


def test_a_declaration_that_contradicts_the_resolution_is_refused():
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        assert_declared_matches_resolved,
    )

    snapshot = _snapshot(
        _identity("op", family="rest", action="POST", endpoint="http://h:8081",
                  route_state="static")
    )
    with pytest.raises(ConnectorIdentityError) as caught:
        assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "GET")})
    assert caught.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"
    assert caught.value.component_key == "op"


def test_the_raw_connector_type_is_canonicalised_before_comparison():
    """Both halves go through one derivation.

    They did not at first: the declared half carries the RAW type
    (``rest_client``) and the resolved half a canonical family (``rest``), so
    every REST component compared unequal to ITSELF and refused — including the
    control. Caught by probing the wiring, which is the only reason a
    fail-closed bug did not reach the suite as a mass refusal.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        assert_declared_matches_resolved,
    )

    snapshot = _snapshot(
        _identity("op", family="rest", action="GET", endpoint="http://h:8081",
                  route_state="static")
    )
    assert assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "GET")})


@pytest.mark.parametrize(
    "route_state", ["dynamic", "unavailable"]
)
def test_an_unresolved_identity_is_never_treated_as_a_contradiction(route_state):
    """'I could not tell' is not evidence the declaration is wrong.

    Refusing here would break every config whose endpoint is bound to an
    environment extension — a legal, common shape.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        assert_declared_matches_resolved,
    )

    snapshot = _snapshot(_identity("op", family="rest", route_state=route_state))
    assert assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "POST")})


def test_the_canonical_sink_comparison_is_currently_TAUTOLOGICAL():
    """RECORDED LIMITATION, pinned so it cannot be mistaken for a working gate.

    At `_build_canonical_symbols` BOTH halves are computed from the same
    `spec.components`: the declared half by `_connector_metadata_from_components`
    and the resolved half by the projection, and for every family both read the
    same config keys. Measured across the shapes that ought to disagree —
    ``method`` versus ``action_type``, both directions, and the database
    equivalent — the guard fires on NONE of them.

    So the comparison is real machinery wired at a place that cannot yet trigger
    it. The declaration only becomes independent evidence when it comes from a
    DIFFERENT source than the config: a live component read back for reuse, or
    the requirements-derived producer on the recipe route. Supplying that is the
    next unit; this test exists so the gap is a recorded fact rather than an
    assumption, and it FAILS the moment the sink gains a real disagreement.
    """
    from boomi_mcp.authoring.workflow import _connector_metadata_from_components
    from boomi_mcp.categories.components.builders.connector_builder import (
        connector_family_of,
        normalized_identity_projection,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    contradictions = [
        {"connector_type": "rest_client", "method": "POST", "action_type": "GET",
         "base_url": "http://h:8081"},
        {"connector_type": "rest_client", "method": "GET", "action_type": "POST",
         "base_url": "http://h:8081"},
        {"connector_type": "database", "operation_mode": "send", "action_type": "Get",
         "host": "h", "dbname": "d"},
    ]
    for config in contradictions:
        component = IntegrationComponentSpec(key="k", type="connector-action", config=config)
        declared = _connector_metadata_from_components([component])["k"]
        resolved = normalized_identity_projection(config)
        assert connector_family_of(declared[0]) == resolved.family
        assert (declared[1] or "").lower() == (resolved.action or "").lower(), (
            "the declared and resolved halves now disagree at this sink — the "
            "comparison is no longer tautological, so this recorded limitation is "
            "stale and the ledger entry must be updated"
        )


# --- slice C: the comparison against the LIVE component (the non-tautological half)


_CAPTURES = "docs/architecture/evidence/issue-155/captures"


def _live_xml(scenario):
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / _CAPTURES
        / scenario
        / "operation_component.xml"
    )
    assert path.is_file(), f"the archived capture {scenario} is missing"
    return path.read_text()


@pytest.mark.parametrize(
    "scenario,verb",
    [
        ("cap155-e2-post", "POST"),
        ("cap155-e2-put", "PUT"),
        ("cap155-e2-delete", "DELETE"),
        ("cap155-e2-head", "HEAD"),
        ("cap155-e2-options", "OPTIONS"),
        ("cap155-e2-trace", "TRACE"),
    ],
)
def test_the_live_identity_is_read_from_real_platform_xml(scenario, verb):
    """Provenance: these are captures of components the platform actually served.

    Not fixtures written to match the reader — the reader is checked against
    what Boomi stored, which is the only direction that proves anything.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    identity = live_identity_from_component_xml("op", _live_xml(scenario))
    assert identity.family == "rest"
    assert identity.action == verb
    assert identity.source == "live"
    assert identity.resolved


def test_a_request_declaring_GET_over_an_account_POST_is_refused():
    """The case slice C exists for, and the one the tautology could not see.

    A request reusing a component the ACCOUNT stores as a POST, while declaring
    a GET. Both halves now come from different sources, so the disagreement is
    real evidence rather than a config compared with itself.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        assert_declared_matches_resolved,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="op",
        type="connector-action",
        component_id="deadbeef-0000-0000-0000-000000000000",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://host.docker.internal:8081"},
    )
    snapshot = build_connector_resolution_snapshot(
        [component], live_component_xml={"op": _live_xml("cap155-e2-post")}
    )

    with pytest.raises(ConnectorIdentityError) as caught:
        assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "GET")})
    assert caught.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"
    assert "the component stored in the account" in str(caught.value), (
        "the refusal must name its evidence source — 'its own configuration' "
        "would describe the tautological comparison, not this one"
    )

    # CONTROL: the same live reading accepts a declaration that agrees with it.
    assert assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "POST")})


def test_the_live_reading_overrides_a_config_that_would_have_agreed():
    """Non-vacuity for the override itself.

    With the live XML absent the config's own GET is what resolves, and the
    declaration agrees — so the refusal above is caused by the live reading and
    by nothing else.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        assert_declared_matches_resolved,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="op",
        type="connector-action",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://host.docker.internal:8081"},
    )
    without_live = build_connector_resolution_snapshot([component])
    assert without_live.lookup("op").source == "config"
    assert without_live.lookup("op").action == "GET"
    assert assert_declared_matches_resolved(without_live, {"op": ("rest_client", "GET")})


# --- slice C: the apply path reads the account -----------------------------------


class _ClientServing:
    """A Boomi client that serves one component's stored XML, and counts reads."""

    def __init__(self, xml_by_id):
        self._xml = xml_by_id
        self.reads = []


def test_apply_reads_the_account_for_reused_connectors(monkeypatch):
    """The wiring, driven — not the comparison in isolation.

    Everything above proves the comparison CAN refuse. This proves the apply
    path actually hands it the account's answer, which is the step whose absence
    made the whole thing a tautology.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    class _Spec:
        components = [
            IntegrationComponentSpec(
                key="reused_op",
                type="connector-action",
                component_id="deadbeef-0000-0000-0000-000000000000",
                config={"connector_type": "rest_client", "method": "GET",
                        "base_url": "http://host.docker.internal:8081"},
            ),
            IntegrationComponentSpec(
                key="created_op",
                type="connector-action",
                config={"connector_type": "rest_client", "method": "GET",
                        "base_url": "http://host.docker.internal:8081"},
            ),
        ]
        processes = ()

    reads = []

    def _fake_get_xml(client, component_id, *a, **kw):
        reads.append(component_id)
        return {"xml": _live_xml("cap155-e2-post")}

    monkeypatch.setattr(
        "boomi_mcp.categories.components._shared.component_get_xml", _fake_get_xml
    )
    live = ib._live_connector_xml(
        boomi_client=object(),
        spec=_Spec(),
        existing_ids={"reused_op": "deadbeef-0000-0000-0000-000000000000"},
        conflict_policy="reuse",
    )

    # ONLY the reused component is read — a created one has no account-side truth,
    # so a create-only apply adds no platform calls at all.
    assert reads == ["deadbeef-0000-0000-0000-000000000000"]
    assert set(live) == {"reused_op"}


def test_a_failed_account_read_is_recorded_rather_than_dropped(monkeypatch):
    """ARCHITECT: an unreadable reuse failed open past the plan's narrow limit.

    Omitting the key made a read that FAILED indistinguishable from a component
    that is not a reuse at all, so the one case where the account is the
    authority and it could not be consulted looked exactly like the case where
    there is no account authority to consult. Every check this slice adds derives
    from that reading, so the component passed all of them.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    class _Spec:
        components = [
            IntegrationComponentSpec(
                key="reused_op", type="connector-action",
                component_id="deadbeef-0000-0000-0000-000000000000",
                config={"connector_type": "rest_client", "method": "GET",
                        "base_url": "http://h:8081"},
            )
        ]
        processes = ()

    def _boom(client, component_id, *a, **kw):
        raise RuntimeError("platform unavailable")

    monkeypatch.setattr(
        "boomi_mcp.categories.components._shared.component_get_xml", _boom
    )
    reading = ib._live_connector_xml(
        boomi_client=object(), spec=_Spec(),
        existing_ids={"reused_op": "deadbeef-0000-0000-0000-000000000000"},
        conflict_policy="reuse",
    )["reused_op"]
    assert reading["read_failed"] is True
    assert reading["xml"] is None


def test_a_reuse_the_account_could_not_answer_for_is_refused():
    """The refusal the plan specified, which this slice had left unbuilt.

    Its code was withdrawn earlier in the slice for the right reason — nothing
    could produce it — and the condition has since become reachable.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="op", type="connector-action", action="create", component_id="id-1",
        config={"connector_type": "rest_client"},
    )
    with pytest.raises(ConnectorIdentityError) as raised:
        build_connector_resolution_snapshot(
            [component],
            live_component_xml={"op": {"xml": None, "read_failed": True}},
            reused_keys={"op"},
        )
    assert raised.value.code == "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE"

    # THE RECORDED LIMIT, unchanged: never having consulted the account is not
    # the same as having consulted it and failed. A pre-apply surface has no
    # client and must still compile.
    build_connector_resolution_snapshot(
        [component], live_component_xml={}, reused_keys={"op"}
    )


def test_the_snapshot_carries_the_mode_and_the_account_it_describes():
    """AC8j: family, action, MODE and the ACTUAL account.

    Mode was reconstructed by each consumer from the reuse predicate, and the
    account was not carried at all — so a resolution could not say which account
    it described, which is what a later slice binds its grants to.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    reused = IntegrationComponentSpec(
        key="reused", type="connector-action", action="create", component_id="id-1",
        config={"connector_type": "rest_client"},
    )
    created = IntegrationComponentSpec(
        key="created", type="connector-action", action="create",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://h:8081", "path": "/x"},
    )
    document = _live_rest_op('<field id="path" value="/x"/>', verb="GET")
    snapshot = build_connector_resolution_snapshot(
        [reused, created],
        live_component_xml={"reused": {"xml": document, "component_id": "id-1",
                                       "component_version": "7", "read_failed": False}},
        reused_keys={"reused"},
        account_id="acct-16926N",
    )
    resolved = snapshot.lookup("reused")
    assert resolved.mode == "reuse"
    assert resolved.account_id == "acct-16926N"
    assert resolved.component_id == "id-1"
    assert resolved.component_version == "7"
    assert resolved.live_read_failed is False

    # A component that is NOT a reuse carries its declared action as the mode and
    # no account reading at all.
    written = snapshot.lookup("created")
    assert written.mode == "create"
    assert written.component_id is None and written.live_read_failed is None

def test_the_wired_apply_comparison_refuses_a_declared_GET_over_an_account_POST():
    """End to end through `_build_canonical_symbols`, the shared construction."""
    from boomi_mcp.authoring.connector_resolution_snapshot import ConnectorIdentityError
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    class _Spec:
        components = [
            IntegrationComponentSpec(
                key="reused_op",
                type="connector-action",
                component_id="deadbeef-0000-0000-0000-000000000000",
                config={"connector_type": "rest_client", "method": "GET",
                        "base_url": "http://host.docker.internal:8081"},
            )
        ]
        processes = ()

    live = {"reused_op": _live_xml("cap155-e2-post")}
    with pytest.raises(ConnectorIdentityError) as caught:
        from boomi_mcp.authoring.connector_resolution_snapshot import (
            build_connector_resolution_snapshot as _snapshot,
        )

        ib._build_canonical_symbols(
            spec=_Spec(),
            resolution=_snapshot(
                _Spec.components, live_component_xml=live, reused_keys={'reused_op'}
            ),
        )
    assert caught.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"

    # CONTROL: without the account reading, the same spec builds cleanly — so the
    # refusal comes from the account and not from anything in the request.
    assert ib._build_canonical_symbols(
        spec=_Spec(), resolution=ib._request_only_resolution(_Spec())
    ) is not None


# --- slice C: the blank-path refusal ---------------------------------------------
#
# The field is only worth adding if it both arrives and is acted on. These cover
# both halves and the silence in between.


def _rest_symbols(requires_path_binding):
    """Refs are BARE here, matching the compiler's own test convention."""
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )

    def symbol(ref, component_type, **extra):
        return ComponentSymbolV1(
            ref=ref, component_id="id_" + ref, component_type=component_type, **extra
        )

    return SymbolTableV1(
        symbols=(
            symbol("conn_rest", "connector-settings", connector_type="rest"),
            symbol(
                "op_rest_get",
                "connector-action",
                connector_type="rest",
                action_type="GET",
                connection_ref="conn_rest",
                requires_path_binding=requires_path_binding,
            ),
        )
    )


_BARE_CALL = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {"kind": "connector_call", "operation_ref": "op_rest_get", "action": "GET"},
            {"kind": "stop"},
        ],
    },
}


def _compile(symbols):
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    return compile_process_ir_v1(parse_process_ir_v1(_BARE_CALL), symbols)


def test_a_blank_path_operation_called_without_a_binding_is_refused():
    """The refusal exists and fires. Without this the field would be inert."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError

    with pytest.raises(ProcessIRCompileError) as caught:
        _compile(_rest_symbols(True))
    codes = [d.code for d in caught.value.diagnostics]
    assert "PROCESS_IR_SEMANTIC_DYNAMIC_PATH_REQUIRED" in codes, codes


@pytest.mark.parametrize("requires", [False, None])
def test_only_an_explicit_requirement_refuses(requires):
    """`None` is silence, not consent — and not refusal either.

    Most callers build no snapshot at all, so `None` is the common case. Reading
    it as "does not require one" would be fail-open; reading it as "does" would
    refuse every document that never mentioned a path. It must say nothing.
    """
    _compile(_rest_symbols(requires))  # compiles; no refusal


def test_the_refusal_is_reached_through_the_single_entry_point():
    """A gate only one of two entry points enforces is not a gate.

    The module documents that rule for its own passes; this pins that the new
    one lives inside `validate_connector_calls` rather than beside it.
    """
    import ast
    import inspect

    from boomi_mcp.compiler.process_ir import connector_resolution

    tree = ast.parse(
        inspect.getsource(connector_resolution.validate_connector_calls)
    )
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "validate_dynamic_path_required" in called, sorted(called)


# --- slice C: what live QA round 1 found, pinned so it cannot come back ----------


#: THE REAL PLATFORM SHAPE, copied from an archived capture of an executed
#: component (`captures/cap155-e2-post/operation_component.xml`) rather than
#: modelled. The earlier fixtures put the verb directly under `<object>`, which
#: the platform never emits — so every test built on them was exercising a
#: document that cannot exist, and the reader they validated was reading the
#: whole document because nothing here ever told it otherwise.
def _live_rest_op(path_field, verb="GET"):
    return _operation_component(
        "officialboomi-X3979C-rest-prod",
        f'<GenericOperationConfig customOperationType="{verb}" '
        f'operationType="EXECUTE">{path_field}</GenericOperationConfig>',
    )


def _operation_component(subtype, operation_config, outside=""):
    """A connector ACTION in the shape the platform stores.

    `operation_config` is the content of `Operation/Configuration` — the
    family's own operation element. `outside` is anything sitting elsewhere in
    the document, which is where decoys go: the point of scoping the reader is
    that nothing out there speaks for the operation.
    """
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        f'type="connector-action" subType="{subtype}">'
        f"<bns:object>{outside}"
        f"<Operation><Configuration>{operation_config}</Configuration></Operation>"
        "</bns:object></bns:Component>"
    )


def _settings_component(subtype, inner):
    """A connector CONNECTION, which has no operation element at all."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        f'type="connector-settings" subType="{subtype}">'
        f"<bns:object>{inner}</bns:object></bns:Component>"
    )


@pytest.mark.parametrize(
    "path_field,expected",
    [
        ('<field id="path" type="string" value=""/>', "dynamic"),
        ('<field id="path" type="string" value="/admin/v1/clients"/>', "static"),
        ("", "unavailable"),
    ],
)
def test_the_live_reading_reads_the_STORED_path(path_field, expected):
    """QA-155-r1-02, Critical: the live reading DISARMED the blank-path net.

    It read only the subtype and the verb, so every live identity came back
    "static" — and because the snapshot prefers the live reading, a REUSED
    operation the account stores with a blank path reported that no binding was
    required. A process was applied whose connector action carried no Path
    property at all.

    Neither unit was wrong on its own; their COMPOSITION was. That is the second
    time in this slice a composition defect survived tests of both halves.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    assert live_identity_from_component_xml("op", _live_rest_op(path_field)).route_state == expected


def test_a_reused_blank_path_operation_still_requires_a_binding():
    """The end-to-end shape of QA-155-r1-02, through the symbol table."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.materialization import build_symbol_table

    # The config LIES: it claims a static path. The account is the authority.
    component = IntegrationComponentSpec(
        key="op",
        type="connector-action",
        component_id="deadbeef-0000-0000-0000-000000000000",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://h:8081", "path": "/looks-static"},
    )
    snapshot = build_connector_resolution_snapshot(
        [component],
        live_component_xml={"op": _live_rest_op('<field id="path" type="string" value=""/>')},
    )
    table = build_symbol_table([component], connector_resolution_snapshot=snapshot)
    assert [s.requires_path_binding for s in table.symbols] == [True]


def test_the_typed_route_carries_the_snapshot():
    """QA-155-r1-01, High: the refusal fired on NO pre-apply surface.

    Plan, compile and `dry_run` all returned success for a document the wet
    apply then refused, because the typed route's symbol table was built without
    a snapshot — one sink of four was wired. Pinned structurally: the unit tests
    build the table directly and so could never have seen this.
    """
    import ast
    import inspect

    from boomi_mcp.authoring import workflow

    source = inspect.getsource(workflow._validate_processes)
    tree = ast.parse(inspect.cleandoc(source).replace("def _validate_processes", "def f", 1))
    kwargs = {
        kw.arg
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for kw in node.keywords
        if kw.arg
    }
    assert "connector_resolution_snapshot" in kwargs, sorted(kwargs)


def test_a_carried_registered_code_reaches_the_served_surface():
    """QA-155-r1-03, High: the identity code had no reader.

    Both identity codes were registered and raised, and every served mismatch
    still reported `PROCESS_MATERIALIZATION_INTERNAL_ERROR`. The taxonomy's
    raiser guard passed because the guard's own test raises the code directly —
    it never asks whether a SURFACE can emit it.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import ConnectorIdentityError
    from boomi_mcp.categories.integration_builder import _canonical_plan_failure
    from boomi_mcp.errors import CONNECTOR_REPLAY_IDENTITY_MISMATCH

    code, path = _canonical_plan_failure(
        ConnectorIdentityError(CONNECTOR_REPLAY_IDENTITY_MISMATCH, "x", component_key="op")
    )
    assert code == CONNECTOR_REPLAY_IDENTITY_MISMATCH
    # The served path field is a JSON POINTER everywhere else; a component key
    # there made one field mean two things with no discriminator (QA-155-r43-03).
    assert path is None

    # ...and an UNREGISTERED code on an exception must not be published.
    class _Bogus(Exception):
        code = "NOT_A_REGISTERED_CODE"

    published, _ = _canonical_plan_failure(_Bogus("x"))
    assert published != "NOT_A_REGISTERED_CODE"


def test_the_live_comparison_runs_before_the_first_write():
    """QA-155-r1-04, High: it ran inside the mutation loop.

    A root ordered after a created dependency refused with
    `mutation_performed: true`, while this slice's other refusal promises
    nothing was created — two refusals from one slice with opposite guarantees.
    The account read now happens exactly once, in the pre-write pass.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_live_connector_xml"
    ]
    assert len(calls) == 1, (
        f"the account is read from {len(calls)} places (lines {[c.lineno for c in calls]}); "
        "one is the pre-write pass and any other is a second authority"
    )


def test_route_readability_does_not_gate_verb_verification():
    """QA-155-r43-01, High — a hole OPENED by the blank-path fix.

    Adding an `unavailable` route state gave `resolved` a new way to be false,
    and the comparison gated on `resolved` — so an operation whose path could
    not be read stopped having its VERB checked. A component the account stores
    as a PATCH could be declared a GET and applied.

    The two facts are independent: the route answers the path question, the
    family and verb answer the identity question. The per-field checks already
    skip whatever is genuinely unknown, which is the precise version of what the
    gate was doing bluntly.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        assert_declared_matches_resolved,
        live_identity_from_component_xml,
    )

    unreadable_route = live_identity_from_component_xml("op", _live_rest_op("", verb="PATCH"))
    assert unreadable_route.route_state == "unavailable"
    assert unreadable_route.action == "PATCH", "the verb IS known; only the route is not"

    from boomi_mcp.authoring.connector_resolution_snapshot import (
        TrustedConnectorResolutionSnapshotV1,
    )

    snapshot = TrustedConnectorResolutionSnapshotV1(identities=(unreadable_route,))
    with pytest.raises(ConnectorIdentityError):
        assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "GET")})

    # CONTROL: an agreeing declaration still passes, so the refusal is the
    # mismatch and not the unreadable route.
    assert assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "PATCH")})


def test_no_registered_replay_code_is_without_a_production_raiser():
    """QA-155-r43-04, and the rule `errors.py` states for itself.

    `CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE` was registered while its only caller
    was a test — the published-code-nothing-can-produce this module refuses for
    the contract-reference code — and was WITHDRAWN rather than given an invented
    consumer. It is registered again now because the condition became reachable:
    apply reads back a component it will reuse, and that read can fail.

    So the assertion is the PROPERTY, not the historical fact. Pinning "this
    particular code is absent" would have had to be deleted the moment the code
    earned its place, and a pin that must be deleted to make progress protects
    nothing. Every registered code in this family must be raised from a
    NON-TEST source file; that is what was actually wrong the first time.
    """
    from pathlib import Path

    from boomi_mcp.errors import ERROR_TAXONOMY

    src = Path(__file__).resolve().parents[1] / "src"
    registered = {c for c in ERROR_TAXONOMY if c.startswith("CONNECTOR_REPLAY_")}
    for code in sorted(registered):
        hits = [
            path
            for path in src.rglob("*.py")
            if code in path.read_text()
        ]
        assert hits, f"{code} is registered but named in no source file"

    # NON-VACUITY: the sweep must be able to fail. A code named ONLY where it is
    # registered has no raiser, and the loop above would still find its own
    # definition — so the raiser has to be somewhere other than `errors.py`.
    for code in sorted(registered):
        raisers = [
            path
            for path in src.rglob("*.py")
            if path.name != "errors.py" and code in path.read_text()
        ]
        assert raisers, f"{code} is named only where it is registered"


def test_a_dry_run_says_which_checks_it_does_not_perform():
    """QA-155-r44-01: the accepted limitation was invisible to callers.

    Everything a request can decide for itself is decided on the dry path. The
    checks that compare the request against components the ACCOUNT holds run in
    the pre-write pass, which a dry run does not reach — so a dry success that
    will refuse on apply read exactly like one that will not.

    Moving those reads onto the dry path would change what `dry_run` means, which
    is not this slice's call; saying so costs nothing. Pinned because a served
    sentence is machine-facing text a caller may rely on.
    """
    import re
    from pathlib import Path

    module = (
        Path(__file__).resolve().parents[1]
        / "src/boomi_mcp/categories/integration_builder.py"
    )
    source = module.read_text()
    dry_block = source[source.index('planned["dry_run"] = True'):]
    dry_block = dry_block[: dry_block.index("return planned")]

    assert "dry_run=false" in dry_block, "the dry run no longer says how to execute"
    assert "account" in dry_block, (
        "the dry-run message no longer discloses that account-dependent checks "
        "are skipped — a dry success then reads identically to one that will "
        "refuse on apply"
    )
    assert "may still refuse" in dry_block


# --- slice C: what the Stage-2 review found ---------------------------------------


@pytest.mark.parametrize(
    "field",
    [
        '<field id="path" type="string" value=""/>',
        '<field value="" type="string" id="path"/>',   # the order that used to miss
        '<field type="string" value="" id="path"/>',
    ],
)
def test_the_live_reading_is_not_attribute_order_sensitive(field):
    """CDX P1: an order-sensitive regex read raw platform bytes.

    Attribute order carries no meaning in XML, so a reader that depends on it is
    sometimes wrong for a reason no caller could predict — and the wrong answer
    here is the one that silences the blank-path net.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    assert live_identity_from_component_xml("op", _live_rest_op(field)).route_state == "dynamic"


def test_malformed_live_xml_resolves_to_nothing_rather_than_raising():
    """The reader runs on a plan path; raising would make a bad byte fatal."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    identity = live_identity_from_component_xml("op", "<not-xml")
    assert identity.route_state == "unavailable"
    assert not identity.resolved


def test_submitted_raw_xml_outranks_the_declarations_beside_it():
    """CDX P1: a raw create's XML is what gets written.

    The request may declare GET while `config.xml` installs a POST. Reading only
    the declarations let the assertion pass on the thing that was NOT applied.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="raw",
        type="connector-action",
        config={
            "connector_type": "rest_client",
            "method": "GET",
            "xml": _live_rest_op('<field id="path" value="/x"/>', verb="POST"),
        },
    )
    identity = build_connector_resolution_snapshot([component]).lookup("raw")
    assert identity.action == "POST", "the submitted XML is the authority"


def test_an_update_keeps_its_DESIRED_identity_not_its_old_one():
    """CDX P2: an update's live component is the state being CHANGED.

    Preferring it would reject a legitimate POST-to-GET update as a mismatch
    with its own former self.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    updating = IntegrationComponentSpec(
        key="upd",
        type="connector-action",
        action="update",
        component_id="dead-0000",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://h:8081", "path": "/x"},
    )
    identity = build_connector_resolution_snapshot(
        [updating], live_component_xml={"upd": _live_rest_op('<field id="path" value="/x"/>', verb="POST")}
    ).lookup("upd")
    assert identity.action == "GET", "the update's DESIRED verb must survive"
    assert identity.source == "config"


@pytest.mark.parametrize(
    "replacements,expected",
    [
        ([{"name": "k", "target_path": "a"}], "dynamic"),
        ([{"name": "k", "target_path": "a"}, {"name": "k", "target_path": "b"}], "unavailable"),
        ([{"name": "absent", "target_path": "a"}], "unavailable"),
    ],
)
def test_only_a_BUILDABLE_replacement_set_projects_as_a_route(replacements, expected):
    """CDX P2: shape-valid is not builder-valid.

    Duplicate names, or a name with no matching token, pass the shape predicate
    and are refused by `validate_config` — so projecting them as a settled route
    promised one for a component that can never be built.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        normalized_identity_projection,
    )

    identity = normalized_identity_projection(
        {"connector_type": "rest_client", "method": "GET", "base_url": "http://h:8081",
         "path": "/x/{k}", "path_replacements": replacements}
    )
    assert identity.route_state == expected


def test_only_a_component_APPLY_WILL_REUSE_is_read_from_the_account(monkeypatch):
    """The read's population is apply's decision, and only apply's.

    Three answers have been wrong here, each in a different direction. Searching
    three places for an id read a plain create's stray config id and every clone
    collision, refusing both against components the request never touches. Keying
    on `planned_action` missed the opposite corner: an explicit `component_id`
    skips candidate resolution, so a declared create carrying one keeps
    `planned_action="create"` while apply reuses it. The predicate below is the
    one apply itself uses, so there is no third direction to be wrong in.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    reads = []

    def _fake_get_xml(client, component_id, *a, **k):
        reads.append(component_id)
        return {"xml": _live_rest_op('<field id="path" value="/x"/>')}

    monkeypatch.setattr(
        "boomi_mcp.categories.components._shared.component_get_xml", _fake_get_xml
    )

    def _cfg(**extra):
        return {"connector_type": "rest_client", "method": "GET", **extra}

    cases = [
        # (label, component, existing id, policy, apply reuses it?)
        ("explicit id, policy=reuse",
         IntegrationComponentSpec(key="k", type="connector-action", action="create",
                                  component_id="i", config=_cfg()), "i", "reuse", True),
        ("explicit id, policy=clone",
         IntegrationComponentSpec(key="k", type="connector-action", action="create",
                                  component_id="i", config=_cfg()), "i", "clone", False),
        ("explicit id, policy=fail",
         IntegrationComponentSpec(key="k", type="connector-action", action="create",
                                  component_id="i", config=_cfg()), "i", "fail", False),
        ("reference_only, policy=clone",
         IntegrationComponentSpec(key="k", type="connector-action", action="create",
                                  config=_cfg(reference_only=True)), "i", "clone", True),
        ("update",
         IntegrationComponentSpec(key="k", type="connector-action", action="update",
                                  component_id="i", config=_cfg()), "i", "reuse", False),
        ("no existing id",
         IntegrationComponentSpec(key="k", type="connector-action", action="create",
                                  config=_cfg()), None, "reuse", False),
    ]
    for label, component, existing, policy, reused in cases:
        reads.clear()
        live = ib._live_connector_xml(
            boomi_client=object(),
            spec=type("S", (), {"components": [component]})(),
            existing_ids={"k": existing} if existing else {},
            conflict_policy=policy,
        )
        assert bool(live) is reused, f"{label}: read={bool(live)}, apply reuses={reused}"


def test_the_reader_and_apply_cannot_disagree_about_what_a_reuse_is():
    """Non-vacuity, and the structural claim: ONE definition site.

    The predicate was answered in three places — apply's own branch, the #139D
    helper, and the reader. Two of the three were reconstructions, and one of
    those got the corner the third had documented in words. This asserts there
    is exactly one place left that decides it.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
    definitions = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_will_reuse_at_apply"
    ]
    assert len(definitions) == 1, "the reuse predicate has more than one definition"

    # And every consumer ASKS it rather than restating its terms. A restatement
    # looks like a conflict-policy comparison inside a function that decides
    # connector reuse.
    #
    # SCOPED, and the scope is the point rather than a convenience. Running this
    # over the whole module finds three more comparisons, and none of them is
    # this predicate: two are the PROCESS-root path, which has its own reuse rule
    # and its own apply implementation — no producer in this tree sets
    # `reference_only` on a process component, so the connector rule does not
    # even apply there — and the third is the PLAN's own labelling, which is
    # deliberately allowed to differ from apply and whose divergence is the very
    # thing #139D documented. A guard that flagged them would be enumerating a
    # universe it has no authority over. They are recorded in the slice ledger as
    # a sibling sweep with that justification, not silently excluded.
    deciders = {
        "_apply_plan", "_live_connector_xml", "_keys_reused_at_apply",
        "_authored_step_will_reuse",
    }
    functions = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name in deciders
    ]
    assert len(functions) == len(deciders), "a connector-reuse decider went missing"

    offenders = []
    for function in functions:
        for node in ast.walk(function):
            if not isinstance(node, ast.Compare):
                continue
            if isinstance(node.left, ast.Name) and node.left.id == "conflict_policy":
                for comparator in node.comparators:
                    if isinstance(comparator, ast.Constant) and comparator.value == "reuse":
                        offenders.append((function.name, node.lineno))
    assert not offenders, (
        f"a connector-reuse decider restates the predicate instead of asking it: {offenders}"
    )
def test_the_pre_write_read_is_skipped_when_no_root_will_be_compiled():
    """QA, then the review: "there is a root" is not "a root will be compiled".

    The pre-write loop skips any root planned as a reuse, so an apply whose roots
    are all reused builds no canonical symbols and reads none of these bytes. The
    gate is derived from the loop's own skip condition so the two cannot drift.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    source = module.read_text()
    tree = ast.parse(source)
    apply_plan = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_apply_plan"
    )
    guarded = [
        node for node in ast.walk(apply_plan)
        if isinstance(node, ast.IfExp)
        and any(
            isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
            and sub.func.id == "_live_connector_xml"
            for sub in ast.walk(node)
        )
        and isinstance(node.test, ast.Name)
        and node.test.id == "_roots_to_compile"
    ]
    assert len(guarded) == 1, "the account read is not gated on a root that will compile"

    # The gate's own population must exclude exactly what the loop skips.
    assert '_planned_actions.get(_pkey) != "reuse"' in source
    assert '_planned_actions.get(_pkey) == "reuse"' in source
def test_the_account_is_read_once_per_apply_not_once_per_root():
    """CDX P2: N roots x M connectors of platform GETs, and worse — two roots
    could observe DIFFERENT snapshots of the same account."""
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
    apply_plan = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_apply_plan"
    )
    for node in ast.walk(apply_plan):
        if isinstance(node, (ast.For, ast.While)):
            for sub in ast.walk(node):
                if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
                        and sub.func.id == "_live_connector_xml"):
                    raise AssertionError(
                        f"the account is read inside a loop at line {sub.lineno}"
                    )


def test_only_a_family_with_a_bindable_location_projects_a_dynamic_route():
    """The sibling of the builder-validity finding, one level out.

    The replacement branch asked the REST operation validator about every
    family. That validator refuses most non-REST configs, so the answer came out
    right by accident — but a config too incomplete for it to refuse, a
    `database` connector carrying only a verb and a replacement set, projected a
    settled DYNAMIC REST route. That asks the compiler for a path binding the
    family capability gate then refuses as unsupported: two contradictory
    refusals for one component, neither of them the real error.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        normalized_identity_projection,
    )

    for connector_type in ("database", "soap_client", "zzz"):
        identity = normalized_identity_projection(
            {
                "connector_type": connector_type,
                "method": "GET",
                "operation_mode": "execute",
                "path_replacements": [{"name": "k", "target_path": "a"}],
            }
        )
        assert identity.route_state == "unavailable", connector_type


def test_the_capability_join_reaches_every_row_of_the_authority():
    """Non-vacuity: the gate is derived, so the derivation must be total.

    The table is keyed by platform connector type and this module speaks
    canonical families, so the join goes through the resolvers rather than a
    hand-written pair list. A key no resolver recognises would silently
    contribute nothing — fail-closed, but invisibly — so the coverage is
    asserted rather than assumed.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        connector_family_of,
    )
    from boomi_mcp.compiler.process_ir.connector_capabilities import (
        CONNECTOR_FAMILY_CAPABILITIES_V1,
    )

    unresolved = [
        key for key in CONNECTOR_FAMILY_CAPABILITIES_V1 if connector_family_of(key) is None
    ]
    assert not unresolved, f"capability rows unreachable through the resolvers: {unresolved}"
    assert any(
        capability.bindable_locations
        for capability in CONNECTOR_FAMILY_CAPABILITIES_V1.values()
    ), "a gate no row can pass decides nothing"


def test_the_slice_a_deferral_is_discharged_by_the_live_reading():
    """`QA-155-r2-01`: declaring a live database operation as a REST one.

    Deferred out of slice A on the recorded grounds that the authority which
    would close it is the trusted snapshot this slice builds. It is closed by
    the account reading, not by a second enumeration of the untrusted fact —
    which is what the structural-fix rule refused there.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        assert_declared_matches_resolved,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    live_database_operation = (
        '<Component type="connector-action" subType="database">'
        '<object><operation customOperationType="Get"/></object></Component>'
    )
    component = IntegrationComponentSpec(
        key="op",
        type="connector-action",
        component_id="id-1",
        config={"connector_type": "rest_client", "method": "GET"},
    )
    snapshot = build_connector_resolution_snapshot(
        [component], live_component_xml={"op": live_database_operation}
    )
    assert snapshot.lookup("op").family == "database"

    with pytest.raises(ConnectorIdentityError) as raised:
        assert_declared_matches_resolved(snapshot, {"op": ("rest_client", "GET")})
    assert raised.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"


def _entity_bomb(depth=6):
    """A self-contained internal-entity expansion, declared and never fetched."""
    declarations = "".join(
        '<!ENTITY l{0} "{1}">'.format(i, ("&l%d;" % (i - 1)) * 10)
        for i in range(1, depth + 1)
    )
    return (
        '<?xml version="1.0"?><!DOCTYPE d [<!ENTITY l0 "aaaaaaaaaa">'
        + declarations
        + ']><Component subType="officialboomi-X3979C-rest-prod"><object>'
        '<operation customOperationType="&l{0};"/></object></Component>'.format(depth)
    )


def test_an_entity_declaration_is_refused_before_anything_expands():
    """QA: a raw create hands its OWN bytes to this reader.

    So the input is caller-controlled, and a few hundred bytes of internal
    entity declarations measured a 2,300-fold expansion on a plan path. The
    refusal happens at the DECLARATION — the expansion is never performed, which
    is why this is not a size limit.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    for depth in (4, 5, 6, 7):
        identity = live_identity_from_component_xml("op", _entity_bomb(depth))
        assert identity.route_state == "unavailable", depth
        assert identity.action is None, "an expanded value reached the identity"
        assert identity.family is None


def test_the_refusal_is_at_the_DECLARATION_not_at_the_expansion():
    """Non-vacuity, and it discriminates the two implementations.

    A post-parse size check would also refuse a bomb — and would accept this
    document, which declares one tiny entity and never references it. Nothing
    expands here, so anything that refuses it is refusing the declaration
    itself, which is the property that makes the expansion unreachable rather
    than merely bounded.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    body = _operation_component(
        _REST_SUBTYPE,
        '<GenericOperationConfig customOperationType="GET">'
        '<field id="path" value="/x"/></GenericOperationConfig>',
    )
    unused = '<?xml version="1.0"?><!DOCTYPE d [<!ENTITY unused "x">]>' + body
    identity = live_identity_from_component_xml("op", unused)
    assert identity.route_state == "unavailable", (
        "an unreferenced entity declaration was accepted, so the refusal is "
        "keyed on expansion size rather than on the declaration"
    )

    # The CONTROL: the same document without the declaration reads normally, so
    # the refusal above is about the entity and not about the shape.
    control = live_identity_from_component_xml("op", body)
    assert (control.family, control.action, control.route_state) == (
        "rest", "GET", "static",
    )


def test_every_archived_platform_capture_still_reads():
    """The corpus control for a reader that changed twice in one slice.

    These are real platform bytes, captured from executed-green components, and
    they are the only documents in this repository the reader did not author.
    A verb capture must read its verb; the blank-path fixture must read dynamic.
    """
    import json
    from pathlib import Path

    captures = Path(__file__).resolve().parents[1] / (
        "docs/architecture/evidence/issue-155/captures"
    )
    if not captures.is_dir():  # pragma: no cover - the archive is tracked
        pytest.skip("evidence archive absent")

    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    read = {}
    for path in sorted(captures.glob("*/*.xml")):
        identity = live_identity_from_component_xml("cap", path.read_text())
        read[f"{path.parent.name}/{path.name}"] = (identity.action, identity.route_state)

    assert read, "the corpus control read nothing"
    expected = {
        "cap155-e1-p0-blankpath-op/op_p0_readback.xml": (None, "dynamic"),
        "cap155-e2-post/operation_component.xml": ("POST", "static"),
        "cap155-e2-put/operation_component.xml": ("PUT", "static"),
        "cap155-e2-delete/operation_component.xml": ("DELETE", "static"),
        "cap155-e2-head/operation_component.xml": ("HEAD", "static"),
        "cap155-e2-options/operation_component.xml": ("OPTIONS", "static"),
        "cap155-e2-trace/operation_component.xml": ("TRACE", "static"),
    }
    for name, (_, route_state) in expected.items():
        if name not in read:  # a capture set may be re-taken under a new name
            continue
        assert read[name][1] == route_state, f"{name} now reads {read[name]}"
        if name in read and expected[name][0] is not None:
            assert read[name][0] == expected[name][0], f"{name} now reads {read[name]}"
    assert json.dumps(read), "unserializable"


def test_unwritten_submitted_xml_never_outranks_the_account():
    """CDX round 2 P1: a raw create that COLLIDES is planned as a reuse.

    Its submitted XML is then never written — apply returns the existing
    component — so treating that XML as the identity compared the declaration
    against a payload nothing applies, and missed both a stored verb mismatch
    and a stored blank path. The precedence chain now asks one question at every
    step: which bytes will this component ACTUALLY have after apply?
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    submitted = _live_rest_op('<field id="path" value="/submitted"/>', verb="POST")
    account = _live_rest_op('<field id="path" type="string" value=""/>', verb="GET")
    component = IntegrationComponentSpec(
        key="raw", type="connector-action", action="create",
        config={"connector_type": "rest_client", "method": "POST", "xml": submitted},
    )

    reused = build_connector_resolution_snapshot(
        [component], live_component_xml={"raw": account}, reused_keys={"raw"}
    ).lookup("raw")
    assert (reused.action, reused.route_state, reused.source) == ("GET", "dynamic", "live"), (
        "a reuse must take the account's bytes, not the XML nobody writes"
    )

    written = build_connector_resolution_snapshot(
        [component], live_component_xml={"raw": account}, reused_keys=set()
    ).lookup("raw")
    assert (written.action, written.source) == ("POST", "config"), (
        "a real create must still take the XML it submits"
    )


def test_the_precedence_chain_covers_every_way_a_component_gets_its_bytes():
    """Non-vacuity for the chain: every route in must be reachable.

    Four ways one component key can get its bytes, three distinct answers —
    a reuse and a plain non-update read share a rung by design, and that is
    asserted rather than assumed, because an earlier version gave them separate
    rungs and a mutation showed the first one decided nothing.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    submitted = _live_rest_op('<field id="path" value="/s"/>', verb="POST")
    account = _live_rest_op('<field id="path" value="/a"/>', verb="DELETE")
    base = {"connector_type": "rest_client", "method": "GET",
            "base_url": "http://h:8081", "path": "/c"}

    def _resolve(config, action, live, reused):
        component = IntegrationComponentSpec(
            key="k", type="connector-action", action=action, config=config
        )
        return build_connector_resolution_snapshot(
            [component],
            live_component_xml=({"k": account} if live else None),
            reused_keys=({"k"} if reused else set()),
        ).lookup("k")

    rungs = {
        "reuse takes the account":
            _resolve({**base, "xml": submitted}, "create", True, True).action,
        "a write takes its submitted xml":
            _resolve({**base, "xml": submitted}, "create", True, False).action,
        "a non-update read takes the account":
            _resolve(base, "create", True, False).action,
        "an update keeps its desired identity":
            _resolve(base, "update", True, False).action,
    }
    assert rungs == {
        "reuse takes the account": "DELETE",
        "a write takes its submitted xml": "POST",
        "a non-update read takes the account": "DELETE",
        "an update keeps its desired identity": "GET",
    }, rungs
    assert len(set(rungs.values())) == 3, "two rungs collapsed onto one answer"


_BYPASS_BODY = _operation_component(
    "officialboomi-X3979C-rest-prod",
    '<GenericOperationConfig customOperationType="PATCH">'
    '<field id="path" type="string" value=""/></GenericOperationConfig>',
)


@pytest.mark.parametrize(
    "prolog",
    [
        '<!DOCTYPE Component [<!ENTITY x "y">]>',
        '<!DOCTYPE Component [<!ENTITY % p "q">]>',
        '<!DOCTYPE d [<!ENTITY a "aa"><!ENTITY b "&a;&a;">]>',
    ],
)
def test_an_unreadable_submitted_payload_is_refused_not_skipped(prolog):
    """QA: two guards this slice ships, defeated by a prologue about neither.

    Thirty-eight bytes of document-type declaration made the payload unreadable,
    which made the identity empty, which made the comparison SKIP — and the
    platform discards that prologue on write, so what landed was exactly the
    document the control refuses. The tolerance being exploited is correct for
    the account's bytes and inverted for the caller's own.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="raw", type="connector-action", action="create",
        config={"connector_type": "rest_client", "method": "GET",
                "xml": prolog + _BYPASS_BODY},
    )
    with pytest.raises(ConnectorIdentityError) as raised:
        build_connector_resolution_snapshot([component])
    assert raised.value.code == "CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE"


def test_the_unreadable_refusal_has_controls_on_both_sides():
    """Non-vacuity: a refusal that fires on everything decides nothing.

    Three controls. A readable payload that AGREES still applies. A readable one
    that disagrees is still refused as a mismatch, under its own code — the new
    refusal must not swallow the old one. And an unreadable payload on a REUSE
    still applies, because a reuse never writes those bytes and their readability
    is irrelevant to what will exist.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        assert_declared_matches_resolved,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def _apply(xml, reused=None):
        component = IntegrationComponentSpec(
            key="raw", type="connector-action", action="create",
            config={"connector_type": "rest_client", "method": "GET", "xml": xml},
        )
        snapshot = build_connector_resolution_snapshot(
            [component], reused_keys=reused or set()
        )
        assert_declared_matches_resolved(snapshot, {"raw": ("rest_client", "GET")})

    _apply(_BYPASS_BODY.replace("PATCH", "GET"))  # agrees: must not raise

    with pytest.raises(ConnectorIdentityError) as mismatch:
        _apply(_BYPASS_BODY)
    assert mismatch.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH", (
        "the unreadable refusal swallowed the mismatch it must not replace"
    )

    _apply('<!DOCTYPE Component [<!ENTITY x "y">]>' + _BYPASS_BODY, reused={"raw"})


def test_the_unreadable_refusal_reaches_a_caller_under_its_own_code():
    """A code nothing can produce is a promise the system cannot keep.

    This slice already withdrew one code for exactly that, so the reachability of
    a new one is proven through the SHARED construction and the failure mapping a
    caller actually sees — not by grepping for the constant.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import ConnectorIdentityError
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    class _Spec:
        components = [IntegrationComponentSpec(
            key="raw", type="connector-action", action="create",
            config={"connector_type": "rest_client", "method": "GET",
                    "xml": '<!DOCTYPE Component [<!ENTITY x "y">]>' + _BYPASS_BODY},
        )]
        processes = ()

    with pytest.raises(ConnectorIdentityError) as raised:
        ib._build_canonical_symbols(
            spec=_Spec(), resolution=ib._request_only_resolution(_Spec())
        )
    served, _path = ib._canonical_plan_failure(raised.value)
    assert served == "CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE"


def test_no_construction_of_canonical_symbols_can_omit_the_resolution():
    """CDX round 3 P1: three functions build symbols, one carried the context.

    So the FIRST construction on a raw apply resolved identities from the request
    alone and could refuse on submitted XML the account was about to make
    irrelevant. The argument is now required — a caller cannot forget what it has
    no default for — and this asserts that, rather than asserting that today's
    call sites happen to pass it.
    """
    import ast
    import inspect
    from pathlib import Path

    from boomi_mcp.categories import integration_builder as ib

    for name in ("_build_canonical_symbols", "_build_canonical_plan"):
        signature = inspect.signature(getattr(ib, name))
        parameter = signature.parameters["resolution"]
        assert parameter.default is inspect.Parameter.empty, (
            f"{name} defaults `resolution`, so a caller can omit it silently"
        )

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id not in {"_build_canonical_symbols", "_build_canonical_plan"}:
            continue
        assert any(k.arg == "resolution" for k in node.keywords), (
            f"{node.func.id} at line {node.lineno} builds without a resolution"
        )


def test_the_submitted_xml_check_survives_a_components_only_apply():
    """CDX round 3 P1: the bypass reopened one layer out from where it closed.

    The refusal lived in the canonical symbol construction, which only runs when
    a process root will be compiled — so a components-only apply wrote raw
    connector XML without the check ever executing. The RESOLUTION is therefore
    built unconditionally, while the account READ stays gated on there being a
    root to compile, because those are two different questions and only one of
    them is about process roots.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
    apply_plan = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_apply_plan"
    )

    def _inside_a_loop_or_conditional(target):
        for node in ast.walk(apply_plan):
            if isinstance(node, (ast.For, ast.While, ast.If, ast.IfExp)):
                if any(sub is target for sub in ast.walk(node)):
                    return True
        return False

    builds = [
        node for node in ast.walk(apply_plan)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "_build_resolution"
    ]
    assert len(builds) == 1, "the apply-wide resolution is not built exactly once"
    assert not _inside_a_loop_or_conditional(builds[0]), (
        "the resolution is conditional, so an apply shape exists that never "
        "checks the caller's own submitted XML"
    )

    reads = [
        node for node in ast.walk(apply_plan)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id == "_live_connector_xml"
    ]
    assert len(reads) == 1 and _inside_a_loop_or_conditional(reads[0]), (
        "the account read must stay gated — nothing consumes it with no root"
    )


def test_a_reuse_whose_account_read_failed_resolves_to_nothing():
    """CDX round 3 P2: falling through to the config makes discarded bytes win.

    Measured before the fix: a reused operation whose read failed reported a
    dynamic route from its request config and demanded a path binding — from
    values apply is about to throw away.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.materialization import _requires_path_binding

    component = IntegrationComponentSpec(
        key="op", type="connector-action", action="create", component_id="id-1",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://h:8081", "path": "/x/{k}",
                "path_replacements": [{"name": "k", "target_path": "a"}]},
    )
    snapshot = build_connector_resolution_snapshot(
        [component], live_component_xml={}, reused_keys={"op"}
    )
    identity = snapshot.lookup("op")
    assert (identity.family, identity.route_state, identity.source) == (
        None, "unavailable", "live",
    )
    assert _requires_path_binding(snapshot, "op") is None, (
        "a requirement was derived from config apply will discard"
    )

    # CONTROL: the same component NOT reused still resolves from its config.
    projected = build_connector_resolution_snapshot([component]).lookup("op")
    assert projected.route_state == "dynamic" and projected.source == "config"


def test_a_wellformed_document_is_readable_even_when_it_classifies_as_nothing():
    """CDX round 3 P2: readability was inferred from what was FOUND.

    A well-formed component for a family this module does not classify has
    neither family nor action — and those are exactly the unsupported connectors
    the raw-XML escape hatch exists to create. Deriving a fact from a proxy for
    it blocked the documented feature.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
        live_identity_from_component_xml,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    unsupported = (
        '<Component type="connector-settings" subType="a-family-we-do-not-model">'
        '<object><Settings url="https://x"/></object></Component>'
    )
    identity = live_identity_from_component_xml("x", unsupported)
    assert identity.family is None and identity.action is None
    assert identity.readable is True, "well-formed is not the same as classified"
    assert live_identity_from_component_xml("x", "<not-xml").readable is False

    # And the escape hatch it protects: this must NOT refuse.
    build_connector_resolution_snapshot([
        IntegrationComponentSpec(
            key="x", type="connector-settings", action="create",
            config={"connector_type": "a-family-we-do-not-model", "xml": unsupported},
        )
    ])


def test_every_registered_replay_code_reaches_a_SERVED_envelope():
    """QA: registered, raiseable, and emitted by nothing a caller can see.

    The repository already guards that every replay code has a raiser. That
    guard passes while the code is unreachable, because its own test raises the
    exception object directly and never asks whether a SURFACE can emit it —
    which is the failure mode `_canonical_plan_failure`'s own comment records,
    and which recurred the moment a refusal moved outside the one `try` that
    reaches the classifier. This asserts the property the other guard cannot.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import ConnectorIdentityError
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.errors import ERROR_TAXONOMY

    registered = {
        code for code in ERROR_TAXONOMY
        if code.startswith("CONNECTOR_REPLAY_IDENTITY_")
        or code == "CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE"
    }
    assert registered, "the check would be vacuous"

    served = set()
    for code in sorted(registered):
        envelope = ib._pre_write_refusal(
            ConnectorIdentityError(code, "constructed", component_key="k")
        )
        assert envelope["_success"] is False
        assert envelope["hint"], code
        served.add(envelope["error_code"])

    assert registered <= served, {
        "registered but not servable": sorted(registered - served)
    }


def test_the_apply_wide_resolution_refusal_is_classified():
    """The specific reachability hole: raised outside every `try`.

    An AST check, because the defect is structural — the refusal fired and wrote
    nothing, and still reached the caller with `error_code: null` purely because
    of where the raise sat.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
    apply_plan = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_apply_plan"
    )
    build = next(
        n for n in ast.walk(apply_plan)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        and n.func.id == "_build_resolution"
    )
    guarded = [
        t for t in ast.walk(apply_plan)
        if isinstance(t, ast.Try) and any(sub is build for sub in ast.walk(t))
    ]
    assert guarded, "the apply-wide resolution build is raised outside every try"

    classifies = any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        and n.func.id == "_pre_write_refusal"
        for handler in guarded[0].handlers for n in ast.walk(handler)
    )
    assert classifies, "its handler does not serve a classified envelope"


def test_the_plan_surface_reports_where_apply_refuses():
    """QA: `plan` began refusing where its own contract says it reports.

    Its docstring is explicit — planning hands a caller everything wrong with
    their intent at once, not the first thing that stopped it. So the identical
    unreadable payload must become a DIAGNOSTIC there and a REFUSAL at apply,
    and the difference is decided by the surface rather than by the condition.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/authoring/workflow.py"
    tree = ast.parse(module.read_text())
    validate = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_validate_processes"
    )
    handlers = [
        h for t in ast.walk(validate) if isinstance(t, ast.Try)
        for h in t.handlers
        if isinstance(h.type, ast.Name) and h.type.id == "ConnectorIdentityError"
    ]
    assert handlers, "the plan surface lets the refusal escape as a raise"
    assert any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_diag"
        for h in handlers for n in ast.walk(h)
    ), "the plan surface catches the refusal but reports no diagnostic"


def test_a_verb_outside_the_operation_does_not_speak_for_it():
    """The property the scoping fix earns, asserted directly.

    The reader used to parse the whole component, so anything anywhere became
    the operation's verb — and the contradiction refusal this module carries was
    built to contain that. With the reader scoped to the operation's own
    element, a stray attribute elsewhere is not a contradiction; it simply is
    not the operation's verb, and the operation's real verb still resolves.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    document = _operation_component(
        _REST_SUBTYPE,
        '<GenericOperationConfig customOperationType="PATCH">'
        '<field id="path" value="/x"/></GenericOperationConfig>',
        outside='<Unrelated customOperationType="GET"/>'
                '<Somewhere><field id="path" value="/decoy"/></Somewhere>',
    )
    identity = live_identity_from_component_xml("k", document)
    assert identity.action == "PATCH", "a decoy outside the operation won"
    assert identity.route_state == "static", "a decoy path outside the operation won"
    assert identity.action_contradicted is False


@pytest.mark.parametrize(
    "configs,expected",
    [
        ('<GenericOperationConfig customOperationType="PATCH"/>', "PATCH"),
        ('<GenericOperationConfig customOperationType="PATCH"/>'
         '<GenericOperationConfig customOperationType="PATCH"/>', "PATCH"),
        ('<GenericOperationConfig customOperationType="patch"/>'
         '<GenericOperationConfig customOperationType="PATCH"/>', "PATCH"),
        # TWO operation configs naming different verbs IS a contradiction: both
        # are the operation's own, and which one the runtime honours is not
        # established here.
        ('<GenericOperationConfig customOperationType="PATCH"/>'
         '<GenericOperationConfig customOperationType="GET"/>', None),
    ],
)
def test_two_operation_configs_naming_different_verbs_resolve_to_neither(
    configs, expected
):
    """A contradiction is now WITHIN the operation, where it means something."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    identity = live_identity_from_component_xml(
        "k", _operation_component(_REST_SUBTYPE, configs)
    )
    assert identity.action == expected
    assert identity.readable is True

def test_caller_xml_naming_two_verbs_is_refused_not_merely_unsettled():
    """Resolving a contradiction to nothing is fail-open for the CALLER's bytes.

    The comparison skips an unknown action, so a component declared with a read
    verb could install XML that executes a write one. Unknown from the account is
    silence; unknown from the caller is a refusal.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def _component(configs):
        return IntegrationComponentSpec(
            key="raw", type="connector-action", action="create",
            config={"connector_type": "rest_client", "method": "GET",
                    "xml": _operation_component(_REST_SUBTYPE, configs)},
        )

    with pytest.raises(ConnectorIdentityError) as raised:
        build_connector_resolution_snapshot([_component(
            '<GenericOperationConfig customOperationType="DELETE"/>'
            '<GenericOperationConfig customOperationType="GET"/>')])
    assert raised.value.code == "CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE"

    settled = build_connector_resolution_snapshot([_component(
        '<GenericOperationConfig customOperationType="GET"/>')]).lookup("raw")
    assert settled.action == "GET"

def test_the_account_branch_keeps_its_silence_on_a_contradiction():
    """A contradiction in bytes the ACCOUNT served is silence, never a refusal.

    Refusing there would turn a platform-side oddity into an authoring refusal,
    which this module has declined to do since its first line.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    contradictory = _operation_component(
        _REST_SUBTYPE,
        '<GenericOperationConfig customOperationType="PATCH"/>'
        '<GenericOperationConfig customOperationType="GET"/>',
    )
    component = IntegrationComponentSpec(
        key="op", type="connector-action", action="create", component_id="id-1",
        config={"connector_type": "rest_client", "method": "GET"},
    )
    identity = build_connector_resolution_snapshot(
        [component], live_component_xml={"op": contradictory}, reused_keys={"op"}
    ).lookup("op")
    assert identity.action is None and identity.source == "live"

def test_every_unresolvable_component_is_reported_and_the_rest_survive():
    """Raising on the first contradicts the planning surface's report-all rule."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def _raw(key, xml):
        return IntegrationComponentSpec(
            key=key, type="connector-action", action="create",
            config={"connector_type": "rest_client", "method": "GET", "xml": xml},
        )

    unparseable = '<!DOCTYPE C [<!ENTITY x "y">]><Component/>'
    contradictory = _operation_component(
        _REST_SUBTYPE,
        '<GenericOperationConfig customOperationType="DELETE"/>'
        '<GenericOperationConfig customOperationType="GET"/>')
    fine = IntegrationComponentSpec(
        key="fine", type="connector-action", action="create",
        config={"connector_type": "rest_client", "method": "GET",
                "base_url": "http://h:8081", "path": "/x"},
    )

    with pytest.raises(ConnectorIdentityError) as raised:
        build_connector_resolution_snapshot(
            [_raw("a", unparseable), _raw("b", contradictory), fine])
    error = raised.value
    assert [f.component_key for f in error.failures] == ["a", "b"]
    assert [i.component_key for i in error.partial.identities] == ["fine"]

def test_the_planning_summary_can_classify_a_snapshot_refusal():
    """CDX round 4 P2: `is_valid: false`, `error_count: 1`, `codes: []`.

    A summary whose whole purpose is to say what is wrong, saying nothing. The
    counts and codes are seeded from the snapshot diagnostics rather than left
    for a later loop that never sees them.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/authoring/workflow.py"
    tree = ast.parse(module.read_text())
    validate = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_validate_processes"
    )
    seeded = {"errors": False, "codes": False}
    for node in ast.walk(validate):
        if not isinstance(node, ast.Assign) or not node.targets:
            continue
        target = node.targets[0]
        name = getattr(target, "id", None) or getattr(
            getattr(target, "target", None), "id", None
        )
        if name is None and hasattr(target, "id"):
            name = target.id
        if name not in seeded:
            continue
        if any(
            isinstance(sub, ast.Name) and sub.id == "snapshot_diagnostics"
            for sub in ast.walk(node.value)
        ):
            seeded[name] = True
    # AnnAssign carries the target differently.
    for node in ast.walk(validate):
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "codes":
            if node.value is not None and any(
                isinstance(sub, ast.Name) and sub.id == "snapshot_diagnostics"
                for sub in ast.walk(node.value)
            ):
                seeded["codes"] = True
    assert seeded["errors"], "the error count ignores snapshot failures"
    assert seeded["codes"], "the served code list ignores snapshot failures"


def _raw_component(subtype, inner, kind="connector-settings"):
    """Real-shape component. For an ACTION, `inner` is the operation config."""
    if "connector-action" in kind:
        return _operation_component(subtype, inner)
    return _settings_component(subtype, inner)


_REST_SUBTYPE = "officialboomi-X3979C-rest-prod"


@pytest.mark.parametrize(
    "label,kind,connector_type,subtype,inner,refused",
    [
        # NAMES NOTHING — a connection has no operation type to name.
        ("rest connection", "connector-settings", "rest_client", _REST_SUBTYPE,
         '<RestConnectionSettings url="http://h:8081"/>', False),
        ("database connection", "connector-settings", "database", "database",
         "<DatabaseConnectionSettings/>", False),
        ("unmodelled family", "connector-settings", "zzz", "zzz", "<Settings/>", False),
        # A REST CONNECTOR ACTION MUST NAME ITS VERB. Scoped to REST because REST
        # is the family whose verb location has been MEASURED — this repository's
        # own builders put the database verb in the element name and SOAP's in
        # `operationType`, so a wider scope refused raw-XML replay of the
        # platform's own bytes for both.
        ("rest action with its verb removed", "connector-action", "rest_client",
         _REST_SUBTYPE, '<GenericOperationConfig/>', True),
        ("rest action with an empty verb", "connector-action", "rest_client",
         _REST_SUBTYPE, '<GenericOperationConfig customOperationType=""/>', True),
        ("rest action with a whitespace verb", "connector-action", "rest_client",
         _REST_SUBTYPE, '<GenericOperationConfig customOperationType="   "/>', True),
        # MIXED blank and nonblank. Filtering blanks out before counting made
        # these collapse to a singleton, so a decoy attribute elsewhere in the
        # document bypassed the missing-verb refusal while the verb actually
        # installed stayed blank. Both orders, because the first version of the
        # reader was order-sensitive and this one must not be.
        ("rest action, blank verb plus a nonblank decoy", "connector-action",
         "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType=""/>'
         '<GenericOperationConfig customOperationType="GET"/>', True),
        ("rest action, nonblank decoy before a blank verb", "connector-action",
         "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType="GET"/>'
         '<GenericOperationConfig customOperationType=""/>', True),
        # A DATABASE action carrying REST-shaped configuration names no database
        # verb at all — its verb is the element, and no database action element
        # is present. Refused for naming none, which is the same rule REST gets.
        #
        # This cell read `False` while settledness was REST-only, and it was
        # right then for the wrong reason: the reader was misreading an
        # irrelevant attribute as a verb, so refusing would have been refusing a
        # misreading. Now each family reads only its own element, and a database
        # action with no database action element genuinely settles nothing.
        ("database action carrying rest-shaped config", "connector-action",
         "database", "database",
         '<GenericOperationConfig customOperationType=""/>'
         '<GenericOperationConfig customOperationType="GET"/>', True),
        ("database action, verb in the element name", "connector-action",
         "database", "database", "<DatabaseGetAction/>", False),
        # SOAP uses the SAME element as REST — measured from the builder, after
        # a fixture here invented `SoapOperationConfig`, which is the third
        # element name in this file that the platform does not emit.
        ("soap action, verb in operationType", "connector-action", "soap_client",
         "wssoapclientsdk", '<GenericOperationConfig operationType="EXECUTE"/>', False),
        ("unmodelled-family action, no verb", "connector-action", "zzz", "zzz",
         "<ZzzOperationConfig/>", False),
        # NAMES ONE — settled.
        ("one verb", "connector-action", "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType="GET"/>', False),
        ("the same verb twice", "connector-action", "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType="GET"/>'
         '<GenericOperationConfig customOperationType="GET"/>', False),
        ("the same verb in two cases", "connector-action", "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType="get"/>'
         '<GenericOperationConfig customOperationType="GET"/>', False),
        # NAMES TWO — settles nothing, and that is the caller's to fix.
        ("two verbs on an operation", "connector-action", "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType="PATCH"/>'
         '<GenericOperationConfig customOperationType="GET"/>', True),
        # A CONNECTION HAS NO OPERATION ELEMENT, so nothing inside one is the
        # operation's verb and there is nothing to contradict. Under the old
        # whole-document scope this refused; that was the scope defect, not a
        # property worth keeping.
        ("operation configs inside a connection", "connector-settings",
         "rest_client", _REST_SUBTYPE,
         '<GenericOperationConfig customOperationType="PATCH"/>'
         '<GenericOperationConfig customOperationType="GET"/>', False),
    ],
)
def test_only_a_CONTRADICTED_submitted_document_is_refused(
    label, kind, connector_type, subtype, inner, refused
):
    """QA: the refusal keyed on "no action resolved", which has two causes.

    A document naming TWO operation types settles nothing. A document naming
    NONE — every raw-XML connector connection — settles nothing about a fact it
    was never going to state, and leaves it to the configuration beside it. The
    predicate could not tell them apart, so every raw connection was refused
    with a message telling its author to supply one operation type instead of
    the several they had not written.

    This is the breadth control the QA round asked for: one axis, every cell
    named, with the boundary in both directions.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="k", type=kind, action="create",
        config={"connector_type": connector_type,
                "xml": _raw_component(subtype, inner, kind)},
    )
    if refused:
        with pytest.raises(ConnectorIdentityError) as raised:
            build_connector_resolution_snapshot([component])
        assert raised.value.code == "CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE", label
    else:
        build_connector_resolution_snapshot([component])  # must not raise


def test_the_contradiction_is_recorded_by_the_reader_not_inferred_downstream():
    """Non-vacuity: an absent verb and a contradicted one are distinguishable.

    Both leave `action` empty, and only one of them is the caller's mistake.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    none_named = live_identity_from_component_xml(
        "k", _settings_component(_REST_SUBTYPE,
                                 '<RestConnectionSettings url="http://h"/>')
    )
    two_named = live_identity_from_component_xml(
        "k", _operation_component(
            _REST_SUBTYPE,
            '<GenericOperationConfig customOperationType="PATCH"/>'
            '<GenericOperationConfig customOperationType="GET"/>')
    )
    assert none_named.action is None and two_named.action is None
    assert none_named.action_contradicted is False
    assert two_named.action_contradicted is True
    assert live_identity_from_component_xml("k", "<not-xml").action_contradicted is None

def test_the_served_summary_describes_both_ways_a_document_fails_to_settle():
    """QA (low): the served row still said only "cannot be read".

    The code is now also raised for a document that WAS read, and that row is
    its only served description — the code appears nowhere else in the catalog.
    """
    from boomi_mcp.errors import ERROR_TAXONOMY

    summary = ERROR_TAXONOMY["CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE"].summary
    assert "settle" in summary
    assert "more than one operation type" in summary


def test_the_verb_rule_reads_the_PAYLOAD_type_not_the_declaration():
    """QA: the platform installs the bytes, so the bytes decide what they are.

    Declaring a connection while submitting a connector-action payload installs
    an ACTION — the platform reads the document. Selecting the rule by the
    request's declaration therefore let one mis-declared field skip it, and the
    verb-less operation it left behind was reusable by a later process.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    action_payload = _raw_component(_REST_SUBTYPE, "<operation/>", "connector-action")

    for declared in ("connector-settings", "connector-action"):
        component = IntegrationComponentSpec(
            key="k", type=declared, action="create",
            config={"connector_type": "rest_client", "xml": action_payload},
        )
        with pytest.raises(ConnectorIdentityError):
            build_connector_resolution_snapshot([component])

    # CONTROL: a genuine connection payload is unaffected by either declaration.
    connection_payload = _raw_component(
        _REST_SUBTYPE, '<RestConnectionSettings url="http://h:8081"/>', "connector-settings"
    )
    for declared in ("connector-settings", "connector-action"):
        build_connector_resolution_snapshot([
            IntegrationComponentSpec(
                key="k", type=declared, action="create",
                config={"connector_type": "rest_client", "xml": connection_payload},
            )
        ])


def test_the_verb_scope_names_only_the_family_whose_verb_location_is_measured():
    """Non-vacuity for the scope, asserted against this repo's own builders.

    The previous scope was "families this module models", which was asserted in
    a comment and false for two of the three. The builders are the authority:
    REST emits `customOperationType`, the database family emits the verb AS the
    element name, SOAP carries `operationType`.
    """
    from pathlib import Path

    builder = Path(__file__).resolve().parents[1] / (
        "src/boomi_mcp/categories/components/builders/connector_builder.py"
    )
    source = builder.read_text()
    assert 'GenericOperationConfig customOperationType="{method}"' in source, (
        "the REST builder no longer emits the attribute this rule reads"
    )
    assert "<DatabaseGetAction" in source and "<DatabaseSendAction" in source, (
        "the database verb is no longer the element name; re-measure the scope"
    )

    snapshot = Path(__file__).resolve().parents[1] / (
        "src/boomi_mcp/authoring/connector_resolution_snapshot.py"
    )
    text = snapshot.read_text()
    assert "_FAMILIES_WITH_A_KNOWN_VERB_LOCATION" in text, (
        "the verb rule's scope is not named by the measured-family set"
    )
    # And the set is exactly the families whose verb location this file derives.
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        _FAMILIES_WITH_A_KNOWN_VERB_LOCATION as measured,
    )

    assert measured == {"rest", "soap_client", "database"}, measured
    assert "GenericOperationConfig" in text, "the REST/SOAP element is not named"
    assert "DatabaseGetAction" in text, "the database action element is not named"


def test_the_unsettled_reasons_have_exactly_one_authority():
    """QA: the served enumeration reached its second instance.

    Three copies existed — the taxonomy summary, this module's messages, and the
    planning remediation — and adding a third reason left all three describing
    two. The structural fix is one list that every consumer reads.
    """
    # The authority lives in `errors`, the LOWEST layer, not beside the raiser.
    # It sat beside the raiser for one commit and the served taxonomy reached up
    # for it during its own import, which is a cycle every test in this suite is
    # blind to because they all import `errors` first.
    from boomi_mcp.errors import (
        ERROR_TAXONOMY,
        SUBMITTED_XML_UNSETTLED_REASONS,
        submitted_xml_unsettled_summary,
    )

    assert len(SUBMITTED_XML_UNSETTLED_REASONS) >= 3
    summary = submitted_xml_unsettled_summary()
    for reason in SUBMITTED_XML_UNSETTLED_REASONS:
        assert reason in summary, reason

    served = ERROR_TAXONOMY["CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE"].summary
    assert summary in served, "the served summary is not derived from the reasons"

    # NON-VACUITY: a reason added to the tuple must reach the served text without
    # anyone editing the served text.
    from boomi_mcp import errors as errors_module

    original = SUBMITTED_XML_UNSETTLED_REASONS
    try:
        errors_module.SUBMITTED_XML_UNSETTLED_REASONS = original + ("a planted reason",)
        assert "a planted reason" in errors_module._submitted_xml_unreadable_summary()
    finally:
        errors_module.SUBMITTED_XML_UNSETTLED_REASONS = original

    # And the snapshot module must READ it rather than keep a copy.
    from boomi_mcp.authoring import connector_resolution_snapshot as snapshot

    assert snapshot.SUBMITTED_XML_UNSETTLED_REASONS is SUBMITTED_XML_UNSETTLED_REASONS


@pytest.mark.parametrize(
    "label,stored,resolves_to",
    [
        ("one verb",
         '<GenericOperationConfig customOperationType="GET">'
         '<field id="path" type="string" value=""/></GenericOperationConfig>', "GET"),
        # THE REGRESSION. A component the account stores with one real verb and a
        # stray blank must still resolve, or every check derived from the
        # snapshot goes quiet for it.
        ("one verb beside a blank",
         '<GenericOperationConfig customOperationType="">'
         '<field id="path" type="string" value=""/></GenericOperationConfig>'
         '<GenericOperationConfig customOperationType="GET"/>', "GET"),
        ("two real verbs — genuinely unresolvable",
         '<GenericOperationConfig customOperationType="PATCH">'
         '<field id="path" type="string" value=""/></GenericOperationConfig>'
         '<GenericOperationConfig customOperationType="GET"/>', None),
        ("a blank alone",
         '<GenericOperationConfig customOperationType="">'
         '<field id="path" type="string" value=""/></GenericOperationConfig>', None),
    ],
)
def test_the_account_rung_resolves_independently_of_what_the_caller_rung_refuses(
    label, stored, resolves_to
):
    """CDX round 6 + QA H10: one predicate served two rungs facing opposite ways.

    Widening "unsettled" is ADDITIVE where it refuses caller bytes and
    SUBTRACTIVE where it resolves account bytes — resolving less is how a net
    goes silent. Widening the refusal to cover a blank therefore stopped a stored
    component with one real verb and a stray blank from resolving at all, and a
    path-less connector action applied because the blank-path refusal had nothing
    left to fire on.

    This asserts the resolution rung directly, THROUGH the consumer that went
    quiet, so a future widening of the refusal cannot take it down again.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
        live_identity_from_component_xml,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec
    from boomi_mcp.recipes.materialization import _requires_path_binding

    document = _operation_component(_REST_SUBTYPE, stored)
    assert live_identity_from_component_xml("op", document).action == resolves_to, label

    component = IntegrationComponentSpec(
        key="op", type="connector-action", action="create", component_id="id-1",
        config={"connector_type": "rest_client"},
    )
    snapshot = build_connector_resolution_snapshot(
        [component], live_component_xml={"op": document}, reused_keys={"op"}
    )
    # The blank-path net fires only on an explicit True.
    expected_binding = True if resolves_to is not None else None
    assert _requires_path_binding(snapshot, "op") is expected_binding, (
        f"{label}: the blank-path refusal is silent for a stored path-less operation"
    )


def test_the_refusal_message_names_the_condition_that_fired():
    """QA: the message said "more than one operation type" for a document
    naming exactly one, beside a blank."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def _refusal(inner):
        component = IntegrationComponentSpec(
            key="k", type="connector-action", action="create",
            config={"connector_type": "rest_client",
                    "xml": _raw_component(_REST_SUBTYPE, inner, "connector-action")},
        )
        with pytest.raises(ConnectorIdentityError) as raised:
            build_connector_resolution_snapshot([component])
        return str(raised.value)

    two = _refusal('<GenericOperationConfig customOperationType="PATCH"/>'
                   '<GenericOperationConfig customOperationType="GET"/>')
    blank = _refusal('<GenericOperationConfig customOperationType=""/>'
                     '<GenericOperationConfig customOperationType="GET"/>')
    assert "more than one operation type" in two
    assert "alongside a blank one" in blank
    assert "more than one operation type" not in blank, (
        "a document naming exactly one verb is told it named several"
    )


def test_no_resolution_is_built_without_declaring_what_was_expected():
    """The comparison is part of CONSTRUCTION, so nothing can omit it.

    It was a separate call for most of this slice, and the same finding recurred
    on three different routes — each fix reaching the routes in front of me and
    none reaching the next. Two guards failed to close it as well, because a
    guard has to model the population and I modelled it wrong twice: first as
    "modules that build a snapshot" (excluding the route that builds none), then
    as "modules that feed the symbol table" (excluding the routes with no process
    root).

    So the property is no longer "every route calls the comparison" but "a
    resolution cannot exist without one" — and what remains checkable is that
    every construction says what it expected.
    """
    import ast
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "src/boomi_mcp"
    BUILDERS = {
        "build_connector_resolution_snapshot", "_build_resolution",
        "_request_only_resolution",
    }
    undeclared = []
    seen = 0
    for path in src.rglob("*.py"):
        if path.name == "connector_resolution_snapshot.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name not in BUILDERS:
                continue
            seen += 1
            # `_request_only_resolution` is the named no-account affordance and
            # declares nothing by design; every other construction must.
            if name == "_request_only_resolution":
                continue
            if not any(k.arg == "declared" for k in node.keywords):
                undeclared.append(f"{path.name}:{node.lineno}")

    assert seen, "no module builds a resolution; this check would be vacuous"
    assert not undeclared, (
        f"these constructions do not say what they expected, so nothing is "
        f"compared against what the components resolve to: {undeclared}"
    )


def test_the_comparison_cannot_be_removed_from_construction():
    """Non-vacuity for the guard above: the fold has to be real.

    If construction stopped comparing, every call site could still pass
    `declared` and nothing would check it.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="op", type="connector-action", action="create",
        config={"connector_type": "rest_client", "method": "GET",
                "xml": _live_rest_op('<field id="path" value="/x"/>', verb="POST")},
    )
    with pytest.raises(ConnectorIdentityError) as raised:
        build_connector_resolution_snapshot(
            [component], declared={"op": ("rest_client", "GET")}
        )
    assert raised.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"

    # CONTROL: an agreeing declaration builds normally.
    snapshot = build_connector_resolution_snapshot(
        [component], declared={"op": ("rest_client", "POST")}
    )
    assert snapshot.lookup("op").action == "POST"

def test_the_planning_surface_still_reports_a_mismatch_rather_than_raising():
    """Folding the comparison into construction must not turn `plan` into a refuser.

    The mismatch now arrives in the same failure set as every other refusal, so
    it reaches the handler that already converts them to diagnostics — which is
    what keeps that surface's report-everything-at-once contract.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/authoring/workflow.py"
    tree = ast.parse(module.read_text())
    validate = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_validate_processes"
    )
    handlers = [
        h for t in ast.walk(validate) if isinstance(t, ast.Try)
        for h in t.handlers
        if isinstance(h.type, ast.Name) and h.type.id == "ConnectorIdentityError"
    ]
    assert handlers, "the planning surface lets a refusal escape as a raise"
    assert any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "_diag"
        for h in handlers for n in ast.walk(h)
    ), "the planning surface catches the refusal but reports no diagnostic"
    # and the construction it guards declares what it expected
    assert any(
        isinstance(n, ast.Call)
        and (getattr(n.func, "id", None) == "build_connector_resolution_snapshot")
        and any(k.arg == "declared" for k in n.keywords)
        for t in ast.walk(validate) if isinstance(t, ast.Try)
        for n in ast.walk(t)
    ), "the planning surface builds a resolution without declaring an expectation"

@pytest.mark.parametrize(
    "label,subtype,connector_type,config,refused",
    [
        # AMBIGUITY IS AMBIGUITY IN EVERY FAMILY. Scoping settledness to REST
        # left these resolving to nothing, and the comparison SKIPS an unknown
        # action — so the caller's declaration stood unopposed for exactly the
        # documents that settle least.
        ("database Get and Send together", "database", "database",
         "<DatabaseGetAction/><DatabaseSendAction/>", True),
        ("soap config naming no verb", "wssoapclientsdk", "soap_client",
         "<GenericOperationConfig/>", True),
        ("rest config naming no verb", "officialboomi-X3979C-rest-prod",
         "rest_client", "<GenericOperationConfig/>", True),
        # ...but a family whose verb location is NOT measured is still one whose
        # bytes cannot be judged, and the raw-XML escape hatch exists for it.
        ("unmodelled family naming no verb", "zzz", "zzz",
         "<GenericOperationConfig/>", False),
        ("database Get alone", "database", "database", "<DatabaseGetAction/>", False),
        ("soap execute alone", "wssoapclientsdk", "soap_client",
         '<GenericOperationConfig operationType="EXECUTE"/>', False),
    ],
)
def test_submitted_settledness_covers_every_family_whose_verb_we_can_find(
    label, subtype, connector_type, config, refused
):
    """ARCHITECT e2: settledness was REST-only while the reader is family-general.

    What belongs to one family is WHERE the verb lives. Whether the caller's own
    bytes settle it is the same question everywhere the answer is findable.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="k", type="connector-action", action="create",
        config={"connector_type": connector_type,
                "xml": _operation_component(subtype, config)},
    )
    if refused:
        with pytest.raises(ConnectorIdentityError) as raised:
            build_connector_resolution_snapshot([component])
        assert raised.value.code == "CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE", label
    else:
        build_connector_resolution_snapshot([component])


def test_every_mismatch_is_reported_not_only_the_first():
    """ARCHITECT e2: the comparison raised on the first disagreement.

    So the planning surface — whose contract is to hand back everything wrong at
    once — caught one exception and named one component, however many disagreed.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        assert_declared_matches_resolved,
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    components = [
        IntegrationComponentSpec(
            key=key, type="connector-action", action="create",
            config={"connector_type": "rest_client", "method": "GET",
                    "xml": _operation_component(
                        _REST_SUBTYPE,
                        '<GenericOperationConfig customOperationType="POST"/>')},
        )
        for key in ("a", "b", "c")
    ]
    snapshot = build_connector_resolution_snapshot(components)
    with pytest.raises(ConnectorIdentityError) as raised:
        assert_declared_matches_resolved(
            snapshot, {key: ("rest_client", "GET") for key in ("a", "b", "c")}
        )
    assert [f.component_key for f in raised.value.failures] == ["a", "b", "c"]

    # CONTROL: agreement raises nothing at all.
    assert_declared_matches_resolved(
        snapshot, {key: ("rest_client", "POST") for key in ("a", "b", "c")}
    )


def test_the_snapshot_contract_is_CLOSED_where_the_plan_says_it_is():
    """ARCHITECT e2: mode and authority were open optional strings.

    A contract of open strings cannot be violated, which is another way of
    saying it promises nothing — a typo, a value from a future caller, a
    silently-renamed mode all passed. The plan names both as closed literals and
    this asserts they are, in the only way that matters: a bogus value is
    refused by the model rather than by a convention.
    """
    import typing

    import pytest as _pytest

    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityAuthorityV1,
        ResolvedConnectorComponentIdentityV1,
        ResolvedConnectorModeV1,
    )

    assert set(typing.get_args(ResolvedConnectorModeV1)) == {"reuse", "create", "update"}
    assert set(typing.get_args(ConnectorIdentityAuthorityV1)) == {
        "normalized_structured_fields", "submitted_xml", "live_readback_xml",
    }

    for field, bogus in (("mode", "recycle"), ("authority", "vibes")):
        with _pytest.raises(Exception):
            ResolvedConnectorComponentIdentityV1(component_key="k", **{field: bogus})


@pytest.mark.parametrize(
    "label,live,reused,expected_authority,expected_mode",
    [
        ("the account answered", True, True, "live_readback_xml", "reuse"),
        ("the caller's own xml answered", False, False, "submitted_xml", "create"),
    ],
)
def test_the_authority_is_set_by_whichever_reader_actually_answered(
    label, live, reused, expected_authority, expected_mode
):
    """Which artifact answered is load-bearing, not descriptive.

    A declaration compared against the configuration it was derived from cannot
    disagree, so only a live readback makes the comparison independent evidence —
    and the refusal message already says which it was. Recording it as a closed
    field means a consumer can act on it rather than parse the sentence.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    document = _live_rest_op('<field id="path" value="/x"/>', verb="GET")
    config = {"connector_type": "rest_client"}
    if not live:
        config = {**config, "xml": document}
    component = IntegrationComponentSpec(
        key="a", type="connector-action", action="create",
        component_id="i" if live else None, config=config,
    )
    snapshot = build_connector_resolution_snapshot(
        [component],
        live_component_xml={"a": {"xml": document, "read_failed": False}} if live else {},
        reused_keys={"a"} if reused else set(),
        account_id="acct-X",
    )
    identity = snapshot.lookup("a")
    assert identity.authority == expected_authority, label
    assert identity.mode == expected_mode
    assert snapshot.account_scope == "acct-X"


def test_a_snapshot_with_no_account_says_so_rather_than_implying_one():
    """Strict scope: `None` is an answer, and a different one from a value."""
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )

    assert build_connector_resolution_snapshot([]).account_scope is None
    assert build_connector_resolution_snapshot([], account_id="a").account_scope == "a"


def test_the_family_comes_from_the_parsed_root_not_from_the_raw_text():
    """CDX terminal: a comment naming another family hijacked the family hint.

    The hint was a regex over the raw document, and unscoped text is not the
    document. With the wrong family the real operation element was ignored, so
    the identity resolved WITH a family and WITHOUT an action — and the
    comparison skips an unknown action, which is the fail-open.

    Removed rather than hardened: the walk already parses the root, and every
    text-scan variant of this defect goes with the regex.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    body = _live_rest_op('<field id="path" value="/x"/>', verb="POST")
    for prefix in ('<!-- subType="database" -->', "<?xml version='1.0'?>", ""):
        identity = live_identity_from_component_xml("k", prefix + body)
        assert identity.family == "rest", prefix
        assert identity.action == "POST", prefix


@pytest.mark.parametrize(
    "label,config,bound",
    [
        ("rest base_url", {"connector_type": "rest_client", "method": "GET"}, True),
        ("database host", {"connector_type": "database", "operation_mode": "get"}, True),
        ("soap endpoint", {"connector_type": "soap_client",
                           "operation_mode": "execute"}, True),
    ],
)
def test_every_family_reports_an_extension_bound_endpoint(label, config, bound):
    """CDX terminal: only the two REST keys were checked.

    So a database host or SOAP endpoint bound to an environment extension was
    advertised as a PINNED route — the opposite of what the flag exists to say.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    key = {"rest base_url": "base_url", "database host": "host",
           "soap endpoint": "endpoint_url"}[label]
    identity = normalized_identity_projection({**config, key: SET_BY_EXTENSION})
    assert identity.extension_bound is bound, label
    assert identity.route_state == "unavailable"


def test_detecting_an_extension_does_not_change_which_field_is_the_endpoint():
    """Non-vacuity: the first version answered both questions from one scan.

    That truncated the database family's composed host/port/database endpoint,
    which its own test caught — the two questions have different answers and
    only one of them is family-specific.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        normalized_identity_projection,
    )

    identity = normalized_identity_projection({
        "connector_type": "database", "operation_mode": "get",
        "host": "host.docker.internal", "port": 11433, "dbname": "Expert",
    })
    assert identity.endpoint == "host.docker.internal:11433/Expert"
    assert identity.extension_bound is False


def test_the_remediation_matches_the_failure_that_produced_it():
    """CDX terminal: folding the comparison in brought a second code through
    one handler, and a verb disagreement was told how to fix parsing."""
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/authoring/workflow.py"
    tree = ast.parse(module.read_text())
    validate = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_validate_processes"
    )
    assert any(
        isinstance(n, ast.IfExp)
        and any(
            isinstance(c, ast.Name) and c.id == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"
            for c in ast.walk(n.test)
        )
        for n in ast.walk(validate)
    ), "the planning remediation does not branch on which failure occurred"


def test_the_recipe_boundary_translates_an_identity_refusal(monkeypatch):
    """CDX terminal: the handler raised TypeError on its own path.

    This test has now been wrong twice, in two different ways, and the second
    way is the instructive one. First it read the AST for a `RecipeError` raise —
    which passes on a call that cannot execute. Then it CONSTRUCTED the error
    the handler was supposed to construct, inside the test — which passes if the
    handler is deleted, keeps an invalid signature, or never catches at all.

    Both versions avoided the production code. This one calls it: snapshot
    construction is made to raise, and the assertion is on what `_compile_processes`
    actually does with that.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
    )
    from boomi_mcp.errors import CONNECTOR_REPLAY_IDENTITY_MISMATCH
    from boomi_mcp.recipes import engine
    from boomi_mcp.recipes.engine import RecipeError

    def _refuse(*_args, **_kwargs):
        raise ConnectorIdentityError(
            CONNECTOR_REPLAY_IDENTITY_MISMATCH,
            "component 'op' is declared with action 'GET', but resolves to 'POST'",
            component_key="op",
        )

    monkeypatch.setattr(
        "boomi_mcp.authoring.connector_resolution_snapshot"
        ".build_connector_resolution_snapshot",
        _refuse,
    )

    composed = type("C", (), {"process_roots": ()})()
    with pytest.raises(RecipeError) as raised:
        engine._compile_processes(
            composed=composed, components=(), connector_metadata={}, resolver=None,
        )

    diagnostics = raised.value.diagnostics
    assert diagnostics, "the refusal reached the recipe envelope with no diagnostic"
    assert diagnostics[0].cause_codes == (CONNECTOR_REPLAY_IDENTITY_MISMATCH,)
    assert diagnostics[0].target == "op"

def test_a_decoy_path_beside_the_operation_config_does_not_settle_its_route():
    """CDX terminal: candidates were filtered by family and their paths were not.

    So a sibling element's `field id="path"` could make a genuinely path-less
    operation resolve as a settled static route, and the blank-path refusal —
    which fires only on an explicit requirement — had nothing to fire on.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )

    with_decoy = _operation_component(
        _REST_SUBTYPE,
        '<Other><field id="path" value="/decoy"/></Other>'
        '<GenericOperationConfig customOperationType="GET"/>',
    )
    identity = live_identity_from_component_xml("k", with_decoy)
    assert identity.action == "GET"
    assert identity.route_state == "unavailable", (
        "a sibling's path settled the operation's route"
    )

    owned = _operation_component(
        _REST_SUBTYPE,
        '<GenericOperationConfig customOperationType="GET">'
        '<field id="path" value="/real"/></GenericOperationConfig>',
    )
    assert live_identity_from_component_xml("k", owned).route_state == "static"


def test_an_extension_bound_endpoint_is_seen_from_the_live_projection_too():
    """CDX terminal: scanning only the config replaced a resolved-endpoint check.

    A config with no endpoint of its own, whose account reading is extension
    bound, reported a static mintable route literally equal to the marker.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    identity = normalized_identity_projection(
        {"connector_type": "rest_client", "method": "GET"},
        live_projection={"url": SET_BY_EXTENSION},
    )
    assert identity.extension_bound is True
    assert identity.route_state == "unavailable"
    assert identity.endpoint != SET_BY_EXTENSION


@pytest.mark.parametrize(
    "label, config, expected_endpoint",
    [
        (
            "database declares a host and composes its endpoint",
            {
                "connector_type": "database",
                "operation_mode": "get",
                "host": "db.internal",
                "port": 1433,
                "dbname": "SALES",
            },
            "db.internal:1433/SALES",
        ),
        (
            "soap declares its own endpoint field",
            {
                "connector_type": "soap_client",
                "operation_mode": "execute",
                "endpoint_url": "http://soap.internal/svc",
            },
            "http://soap.internal",
        ),
    ],
)
def test_a_declared_endpoint_is_never_replaced_by_a_stale_live_marker(
    label, config, expected_endpoint
):
    """CDX terminal: detection was family-aware and selection was not.

    Splitting "is this extension bound" from "which field IS the endpoint" was
    right, but only the first question learned the family. A database config
    declaring `host` therefore suppressed the live marker as intended, then fell
    through a `base_url`/`url` lookup that found nothing and took the live value
    anyway — so the literal marker string survived as a static, mintable
    endpoint for the two families whose endpoint field is neither of those.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    identity = normalized_identity_projection(
        config, live_projection={"url": SET_BY_EXTENSION}
    )

    assert identity.endpoint == expected_endpoint, label
    assert identity.extension_bound is False, label
    assert identity.route_state == "static", label


def test_the_endpoint_field_table_stays_inside_its_family_authority():
    """Structural pin for the recurring class: the endpoint table is a SUBSET.

    Which fields of a family may carry the extension marker is already decided
    by the three placeholder sets the builders validate against. The endpoint
    table names the endpoint members of those sets, so it is pinned to them
    rather than standing beside them as a fourth independent hand-list: if a
    builder stops accepting the marker on a field, this test forces the endpoint
    table to be re-examined instead of drifting.
    """
    from boomi_mcp.categories.components.builders import connector_builder as cb

    authority = {
        "rest": cb._REST_EXTENSION_BOUND_PLACEHOLDER_FIELDS,
        "soap_client": cb._SOAP_EXTENSION_BOUND_PLACEHOLDER_FIELDS,
        "database": cb._DB_EXTENSION_BOUND_PLACEHOLDER_FIELDS,
    }

    assert set(cb._ENDPOINT_FIELDS_BY_FAMILY) == set(authority)
    for family, fields in cb._ENDPOINT_FIELDS_BY_FAMILY.items():
        assert fields, f"{family} names no endpoint field"
        assert set(fields) <= set(authority[family]), (
            f"{family} names an endpoint field its builder cannot extension-bind: "
            f"{sorted(set(fields) - set(authority[family]))}"
        )


def test_every_family_the_resolver_can_return_has_an_endpoint_field():
    """Coverage derived from the resolver's own case set, never hand-listed."""
    import inspect
    import re

    from boomi_mcp.categories.components.builders import connector_builder as cb

    returned = set(
        re.findall(
            r'return "([a-z_]+)"', inspect.getsource(cb.connector_family_of)
        )
    )
    assert returned, "derivation found no families — the pin would be vacuous"
    # Reading the source can only see LITERAL returns, so the derivation is
    # complete only while every return is a literal. A family returned through a
    # variable would be invisible here and leave this file green.
    body = inspect.getsource(cb.connector_family_of)
    computed = [
        line.strip()
        for line in body.splitlines()
        if re.match(r"\s*return (?!None\b)(?!\")", line)
    ]
    assert not computed, (
        "connector_family_of returns a non-literal; this pin can no longer "
        "derive its universe from the source: " + repr(computed)
    )
    missing = returned - set(cb._ENDPOINT_FIELDS_BY_FAMILY)
    assert not missing, f"families with no endpoint field: {sorted(missing)}"


@pytest.mark.parametrize(
    "config",
    [
        {"connector_type": "rest_client", "method": "GET"},
        {"connector_type": "rest_client", "method": "  patch  "},
        {"connector_type": "rest_client", "method": "TRACE"},
        {"connector_type": "rest_client", "method": 7},
        {"connector_type": "rest_client"},
        {"connector_type": "database", "operation_mode": "get"},
        {"connector_type": "database", "operation_mode": "SEND"},
        {"connector_type": "database", "operation_mode": "upsert"},
        {"connector_type": "database"},
        {"connector_type": "soap_client", "operation_mode": "execute"},
        {"connector_type": "soap_client", "operation_mode": "listen"},
        {"connector_type": "soap_client"},
        {"connector_type": "http", "method": "GET"},
        {"connector_type": "ftp"},
        {},
    ],
)
def test_the_projected_action_agrees_with_the_derivation_it_mirrors(config):
    """Sibling sweep: the mirror was unpinned, so it could drift silently.

    `_normalized_action` deliberately re-derives what
    `authoring.workflow._action_type_from_config` decides, because importing the
    other way would invert the dependency — but nothing asserted the two agree,
    which is the same defect one level up: a hand-model of a fact whose
    authority lives elsewhere, with no pin holding it there.
    """
    from boomi_mcp.authoring.workflow import _action_type_from_config
    from boomi_mcp.categories.components.builders.connector_builder import (
        _normalized_action,
        connector_family_of,
    )

    family = connector_family_of(config.get("connector_type"))
    assert _normalized_action(config, family) == _action_type_from_config(config)


def test_a_soap_component_does_not_compare_unequal_to_itself():
    """The projection agrees with the platform's own spelling of the verb.

    Recorded honestly, because the first version of this docstring claimed a
    harm that live QA then measured to be unreachable: the identity comparison
    casefolds both halves, so the lowercase projection never actually refused a
    SOAP component, and no commit on this branch could have exhibited that. The
    DRIFT was real — the projection disagreed with the derivation it documents
    itself as mirroring — and aligning it moved none of the served leaves. What
    this pins is the agreement, not a refusal that never happened.
    """
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        live_identity_from_component_xml,
    )
    from boomi_mcp.categories.components.builders.connector_builder import (
        normalized_identity_projection,
    )

    resolved = live_identity_from_component_xml(
        "op",
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/"'
        ' type="connector-action" subType="wssoapclientsdk">'
        "<bns:object><Operation><Configuration>"
        '<GenericOperationConfig operationType="EXECUTE"/>'
        "</Configuration></Operation></bns:object></bns:Component>",
    )
    declared = normalized_identity_projection(
        {
            "connector_type": "soap_client",
            "operation_mode": "execute",
            "endpoint_url": "http://soap.internal/svc",
        }
    )

    assert resolved.family == declared.family == "soap_client"
    assert resolved.action == declared.action == "EXECUTE"


def test_a_marker_on_a_later_endpoint_field_still_says_the_account_decides():
    """Live QA cell N1: the two questions reduce the same fields differently.

    "Is the account deciding this route" is true if ANY of the family's endpoint
    fields carries the marker; "which value is the endpoint" takes the first one
    present. Collapsing both onto the first present field lost the marker on a
    later one, so a SOAP config with a concrete endpoint and an extension-bound
    WSDL went from unavailable to a pinned static route.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    identity = normalized_identity_projection(
        {
            "connector_type": "soap_client",
            "operation_mode": "execute",
            "endpoint_url": "http://soap.internal/svc",
            "wsdl_url": SET_BY_EXTENSION,
        }
    )
    assert identity.extension_bound is True
    assert identity.route_state == "unavailable"
    assert identity.endpoint is None

    # Control: both fields concrete still resolves to a static route.
    both = normalized_identity_projection(
        {
            "connector_type": "soap_client",
            "operation_mode": "execute",
            "endpoint_url": "http://soap.internal/svc",
            "wsdl_url": "http://soap.internal/svc?wsdl",
        }
    )
    assert both.extension_bound is False
    assert both.route_state == "static"


@pytest.mark.parametrize(
    "label, live",
    [
        ("marker on the second live key", {"url": "http://concrete/svc", "endpoint": "@MARKER@"}),
        ("marker on the first live key", {"url": "@MARKER@", "endpoint": "http://concrete/svc"}),
    ],
)
def test_a_marker_on_any_live_endpoint_key_still_says_the_account_decides(label, live):
    """The same asymmetry as the declared half, found one field-set over.

    Correcting the declared half to scan every field while the live half kept
    taking the first present key left the identical defect in place: a live
    projection carrying a concrete address beside an extension-bound one
    reported a static route. Both halves now read through one helper.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    resolved = {
        key: (SET_BY_EXTENSION if value == "@MARKER@" else value)
        for key, value in live.items()
    }
    identity = normalized_identity_projection(
        {"connector_type": "soap_client", "operation_mode": "execute"},
        live_projection=resolved,
    )
    assert identity.extension_bound is True, label
    assert identity.route_state == "unavailable", label
    assert identity.endpoint is None, label


def test_both_halves_of_the_extension_question_read_through_one_helper():
    """Non-vacuity for the structural claim: neither half open-codes the scan.

    Counting calls was not enough — live QA showed a mutant that re-open-coded
    the live half and left a spare second call on the config kept this green.
    What matters is WHICH source each call reads, so the check reads the parse
    tree rather than the text.
    """
    import ast
    import inspect
    import textwrap

    from boomi_mcp.categories.components.builders import connector_builder as cb

    tree = ast.parse(textwrap.dedent(inspect.getsource(cb.normalized_identity_projection)))

    # Which names each helper call BINDS, keyed by the source it reads. Finding
    # the call is not enough: both calls are needed for endpoint selection, so a
    # re-open-coded scan can leave them standing and still decide from its own
    # values. What has to hold is that the DECISION consumes what they bound.
    bound_by_source = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        call = node.value
        if not (isinstance(call.func, ast.Name) and call.func.id == "_endpoint_reading"):
            continue
        if not (call.args and isinstance(call.args[0], ast.Name)):
            continue
        names = {t.id for t in ast.walk(node.targets[0]) if isinstance(t, ast.Name)}
        bound_by_source.setdefault(call.args[0].id, set()).update(names)

    decision = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "extension_bound"
            for t in ast.walk(n.targets[0])
        )
    ]
    assert decision, "no assignment to extension_bound was found"
    consumed = {n.id for n in ast.walk(decision[0].value) if isinstance(n, ast.Name)}

    for source in ("config", "_live"):
        assert source in bound_by_source, (
            f"nothing reads {source} through the helper any more"
        )
        assert bound_by_source[source] & consumed, (
            f"the extension decision does not consume what the helper read from "
            f"{source}: it binds {sorted(bound_by_source[source])} but decides from "
            f"{sorted(consumed)}"
        )


def test_the_composer_declares_exactly_the_fields_it_reads():
    """The scan set is only right while the declared field list matches the body."""
    import ast
    import inspect
    import textwrap

    from boomi_mcp.categories.components.builders import connector_builder as cb

    # The universe is the REGISTRY, not one composer named by hand: the code
    # looks the composer up by family, so a second entry without a field list
    # would be invisible to a pin that only ever inspects the first.
    assert cb._COMPOSED_ENDPOINT_BY_FAMILY, "no composers registered — pin vacuous"
    for family, composer in cb._COMPOSED_ENDPOINT_BY_FAMILY.items():
        declared = set(getattr(composer, "reads", ()))
        assert declared, f"{family} registers a composer with no declared fields"
        # PARSED, NOT PATTERN-MATCHED. The derivation was a regular expression
        # requiring double quotes, so a composer reading a single-quoted field
        # name declared a field the scan could not see — and served the marker
        # inside the composed route with this guard and its sibling both green.
        # Live QA found that by writing the single-quoted form, which is why the
        # fix is the language's own parser rather than a second pattern: quoting
        # is one of arbitrarily many spellings a regex must be told about, and
        # the parser already knows them all.
        tree = ast.parse(textwrap.dedent(inspect.getsource(composer)))
        read_in_body = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr != "get":
                continue
            if not isinstance(func.value, ast.Name) or func.value.id != "config":
                continue
            key = node.args[0]
            # A NON-LITERAL KEY IS NOT A FIELD THIS PIN CAN CHECK, and pretending
            # otherwise would be worse than saying so: a computed key means the
            # declaration cannot be verified from the body at all.
            assert isinstance(key, ast.Constant) and isinstance(key.value, str), (
                f"{family}: the composer reads a config key this pin cannot "
                f"resolve statically, so its declaration is unverifiable"
            )
            read_in_body.add(key.value)
        assert read_in_body, f"{family}: derivation found no config reads — pin vacuous"
        assert read_in_body == declared, (
            f"{family}: the composer's declared fields and the fields it reads have "
            f"diverged: declared={sorted(declared)} body={sorted(read_in_body)}"
        )


@pytest.mark.parametrize("field", ["host", "port", "dbname"])
def test_a_marker_in_any_composed_field_disarms_the_database_route(field):
    """Live QA cell: detection covered `host` while composition read three fields.

    A marker in `port` or `dbname` was embedded into the composed value and
    served as a settled static route — `db.internal:SET_BY_EXTENSION/SALES`.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    config = {
        "connector_type": "database",
        "operation_mode": "get",
        "host": "db.internal",
        "port": 1433,
        "dbname": "SALES",
    }
    config[field] = SET_BY_EXTENSION

    identity = normalized_identity_projection(config)
    assert identity.route_state == "unavailable", field
    assert identity.endpoint is None, field
    # Only `host` is extension-bindable for this family; the builder refuses the
    # placeholder on the other two, so claiming a binding there would assert one
    # this family cannot emit. Both fail closed, for different stated reasons.
    assert identity.extension_bound is (field == "host"), field

    concrete = normalized_identity_projection(
        {**config, field: {"host": "db.internal", "port": 1433, "dbname": "SALES"}[field]}
    )
    assert concrete.endpoint == "db.internal:1433/SALES", field
    assert concrete.route_state == "static", field


def test_a_secondary_field_alone_does_not_count_as_a_settled_endpoint():
    """Precedence keys on settlement, not on presence.

    A database config carrying a port and no host composes nothing, so the
    account's reading is still the answer. Treating "a scanned field is present"
    as "the config answered" discarded a live marker on exactly that input and
    reported a static route with no endpoint at all.
    """
    from boomi_mcp.categories.components.builders.connector_builder import (
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    identity = normalized_identity_projection(
        {"connector_type": "database", "operation_mode": "get", "port": 1433, "dbname": "S"},
        live_projection={"url": SET_BY_EXTENSION},
    )
    assert identity.extension_bound is True
    assert identity.route_state == "unavailable"

    # Control: once the config DOES settle an endpoint, the live marker is
    # correctly ignored — the live reading fills, it never overrides.
    settled = normalized_identity_projection(
        {
            "connector_type": "database",
            "operation_mode": "get",
            "host": "db.internal",
            "port": 1433,
            "dbname": "S",
        },
        live_projection={"url": SET_BY_EXTENSION},
    )
    assert settled.extension_bound is False
    assert settled.endpoint == "db.internal:1433/S"


def test_only_a_field_the_builder_can_bind_may_claim_an_extension_binding():
    """The placeholder is refused on non-extension fields, so a marker there is
    malformed input rather than an account-decided route. Both fail closed; only
    the bindable field reports a binding. The builder itself is the authority
    consulted here, not a second copy of its rules."""
    import sys

    sys.path.insert(0, "tests")
    from test_database_connector_builder import _minimal_config

    from boomi_mcp.categories.components.builders import connector_builder as cb_module
    from boomi_mcp.categories.components.builders.connector_builder import (
        DatabaseConnectorBuilder,
        SET_BY_EXTENSION,
        normalized_identity_projection,
    )

    for field in cb_module._COMPOSED_ENDPOINT_BY_FAMILY["database"].reads:
        refused_by_builder = (
            DatabaseConnectorBuilder.validate_config(
                _minimal_config(**{field: SET_BY_EXTENSION})
            )
            is not None
        )
        identity = normalized_identity_projection(
            {
                "connector_type": "database",
                "operation_mode": "get",
                "host": "db.internal",
                "port": 1433,
                "dbname": "SALES",
                field: SET_BY_EXTENSION,
            }
        )
        assert identity.route_state == "unavailable", field
        assert identity.extension_bound is not refused_by_builder, field


def _module_with(mutation):
    """Import the builder from a copied tree with one substitution applied."""
    import os
    import pathlib
    import shutil
    import subprocess
    import sys
    import tempfile

    root = pathlib.Path(__file__).resolve().parents[1]
    rel = "boomi_mcp/categories/components/builders/connector_builder.py"
    tmp = tempfile.mkdtemp()
    try:
        shutil.copytree(root / "src", pathlib.Path(tmp) / "src")
        target = pathlib.Path(tmp) / "src" / rel
        text = target.read_text()
        old, new = mutation
        assert text.count(old) == 1, "mutation did not match exactly once"
        target.write_text(text.replace(old, new, 1))
        return subprocess.run(
            [sys.executable, "-c", "import boomi_mcp.categories.components.builders.connector_builder"],
            env={**os.environ, "PYTHONPATH": str(pathlib.Path(tmp) / "src")},
            capture_output=True,
            text=True,
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.parametrize(
    "label, reads",
    [("a bare string", '"host"'), ("empty", "()"), ("names that are not strings", "(1, 2)")],
)
def test_registration_refuses_a_composer_whose_declared_fields_are_malformed(label, reads):
    """Live QA: the check tested truthiness, so `reads = "host"` imported clean
    and the scan then looked for four one-letter fields, finding none — the
    marker went back into the composed route with every guard green."""
    result = _module_with(
        (
            '_compose_database_endpoint.reads = ("host", "port", "dbname")',
            "_compose_database_endpoint.reads = " + reads,
        )
    )
    assert result.returncode != 0, f"{label}: registration accepted it"
    assert "must be a non-empty tuple of field names" in result.stderr, label


def test_an_empty_composer_registry_still_imports():
    """Live QA: cleaning up the registration loop's variables made the whole
    module unimportable the moment the registry was empty — a whole-surface
    import failure behind a name error that explains nothing."""
    result = _module_with(
        (
            "_validated_composer_registry(\n    {\"database\": _compose_database_endpoint}\n)",
            "_validated_composer_registry({})",
        )
    )
    assert result.returncode == 0, (
        "an empty registry must import cleanly, not raise: " + result.stderr
    )


def test_a_composer_without_declared_fields_refuses_rather_than_scanning_nothing():
    """The read must not default: answering "reads nothing" switches the
    placeholder refusal off for that family instead of failing closed."""
    from boomi_mcp.categories.components.builders import connector_builder as cb

    def undeclared(config):
        return cb._compose_database_endpoint(config)

    saved = cb._COMPOSED_ENDPOINT_BY_FAMILY["database"]
    cb._COMPOSED_ENDPOINT_BY_FAMILY["database"] = undeclared
    try:
        with pytest.raises(AttributeError):
            cb.normalized_identity_projection(
                {
                    "connector_type": "database",
                    "operation_mode": "get",
                    "host": "db.internal",
                    "port": 1433,
                    "dbname": cb.SET_BY_EXTENSION,
                }
            )
    finally:
        cb._COMPOSED_ENDPOINT_BY_FAMILY["database"] = saved

    # The registry is restored and the shipped composer still fails closed.
    identity = cb.normalized_identity_projection(
        {
            "connector_type": "database",
            "operation_mode": "get",
            "host": "db.internal",
            "port": 1433,
            "dbname": cb.SET_BY_EXTENSION,
        }
    )
    assert identity.route_state == "unavailable"
    assert identity.endpoint is None
