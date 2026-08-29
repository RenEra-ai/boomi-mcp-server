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
        planned_actions={"reused_op": "reuse", "created_op": "create"},
        existing_ids={"reused_op": "deadbeef-0000-0000-0000-000000000000"},
    )

    # ONLY the reused component is read — a created one has no account-side truth,
    # so a create-only apply adds no platform calls at all.
    assert reads == ["deadbeef-0000-0000-0000-000000000000"]
    assert set(live) == {"reused_op"}


def test_an_unreadable_component_skips_the_comparison_rather_than_refusing(monkeypatch):
    """A transient platform error must not become an authoring refusal."""
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    class _Spec:
        components = [
            IntegrationComponentSpec(
                key="reused_op",
                type="connector-action",
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
    assert ib._live_connector_xml(
        boomi_client=object(),
        spec=_Spec(),
        planned_actions={"reused_op": "reuse"},
        existing_ids={"reused_op": "deadbeef-0000-0000-0000-000000000000"},
    ) == {}
    # and the symbol table still builds, because nothing was contradicted
    assert ib._build_canonical_symbols(spec=_Spec(), live_component_xml={}) is not None


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
        ib._build_canonical_symbols(spec=_Spec(), live_component_xml=live)
    assert caught.value.code == "CONNECTOR_REPLAY_IDENTITY_MISMATCH"

    # CONTROL: without the account reading, the same spec builds cleanly — so the
    # refusal comes from the account and not from anything in the request.
    assert ib._build_canonical_symbols(spec=_Spec(), live_component_xml={}) is not None


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


def _live_rest_op(path_field, verb="GET"):
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="connector-action" subType="officialboomi-X3979C-rest-prod">'
        f'<bns:object><Operation customOperationType="{verb}">'
        f"<GenericOperationConfig>{path_field}</GenericOperationConfig>"
        "</Operation></bns:object></bns:Component>"
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
    was a test, which is exactly the published-code-nothing-can-produce that
    this module refuses for the contract-reference code. It was removed rather
    than given an invented consumer; its real one is the no-client reuse path.
    """
    from pathlib import Path

    from boomi_mcp.errors import ERROR_TAXONOMY

    assert "CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE" not in ERROR_TAXONOMY

    src = Path(__file__).resolve().parents[1] / "src"
    registered = {c for c in ERROR_TAXONOMY if c.startswith("CONNECTOR_REPLAY_")}
    for code in sorted(registered):
        hits = [
            path
            for path in src.rglob("*.py")
            if code in path.read_text()
        ]
        assert hits, f"{code} is registered but named in no source file"


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


def test_only_a_planned_REUSE_is_read_from_the_account(monkeypatch):
    """CDX P1, then QA: the question is not "is there an id" but "is this a reuse".

    An id exists in several places and means several things. A plain create can
    carry a stray `component_id` in its config, where the key binds nothing
    unless `reference_only` is set; and a clone collision has an `existing_id`
    the planner sets DELIBERATELY, to name the component being cloned FROM.
    Reading either one refuses the request against a component it never touches
    — which live QA measured as a refusal of every `conflict_policy="clone"`
    apply. The plan already made this decision; this asks it.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    def _config(**extra):
        return {"connector_type": "rest_client", "method": "GET", **extra}

    reads = []

    def _fake_get_xml(client, component_id, *a, **k):
        reads.append(component_id)
        return {"xml": _live_rest_op('<field id="path" value="/x"/>')}

    monkeypatch.setattr(
        "boomi_mcp.categories.components._shared.component_get_xml", _fake_get_xml
    )

    spec = type("S", (), {"components": [
        IntegrationComponentSpec(key="reused", type="connector-action", config=_config()),
        IntegrationComponentSpec(key="updated", type="connector-action",
                                 action="update", component_id="id-upd", config=_config()),
        IntegrationComponentSpec(key="cloned", type="connector-action", config=_config()),
        IntegrationComponentSpec(key="stray", type="connector-action",
                                 config=_config(component_id="id-stray")),
        IntegrationComponentSpec(key="created", type="connector-action", config=_config()),
    ]})()

    live = ib._live_connector_xml(
        boomi_client=object(),
        spec=spec,
        planned_actions={
            "reused": "reuse",
            "updated": "update",
            "cloned": "create_clone",
            "stray": "create",
            "created": "create",
        },
        existing_ids={
            "reused": "id-reused",
            "updated": "id-upd",
            "cloned": "id-cloned-from",
            "stray": None,
            "created": None,
        },
    )
    assert set(live) == {"reused"}, "something other than a planned reuse was read"
    assert reads == ["id-reused"], f"platform reads beyond the reuse: {reads}"


def test_the_reuse_rule_asks_the_planner_rather_than_searching_for_an_id():
    """Non-vacuity: the guard above passes for a function that reads nothing.

    So this constructs the case the OLD rule got wrong and the new one must get
    right — the same component key, the same id in the same place, differing
    only in what the plan decided about it.
    """
    from boomi_mcp.categories import integration_builder as ib
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    component = IntegrationComponentSpec(
        key="c", type="connector-action", component_id="id-1",
        config={"connector_type": "rest_client", "method": "GET"},
    )
    spec = type("S", (), {"components": [component]})()

    def _run(planned_action):
        seen = []

        class _Shared:
            @staticmethod
            def component_get_xml(client, component_id, *a, **k):
                seen.append(component_id)
                return {"xml": _live_rest_op('<field id="path" value="/x"/>')}

        import sys
        real = sys.modules["boomi_mcp.categories.components._shared"].component_get_xml
        sys.modules["boomi_mcp.categories.components._shared"].component_get_xml = (
            _Shared.component_get_xml
        )
        try:
            ib._live_connector_xml(
                boomi_client=object(), spec=spec,
                planned_actions={"c": planned_action},
                existing_ids={"c": "id-1"},
            )
        finally:
            sys.modules["boomi_mcp.categories.components._shared"].component_get_xml = real
        return seen

    assert _run("reuse") == ["id-1"], "a planned reuse must be read"
    assert _run("create_clone") == [], "a clone names what it copies, not what it uses"
    assert _run("create") == [], "a create has no account-side truth"


def test_the_pre_write_read_is_skipped_when_nothing_consumes_it():
    """QA: the hoist made the call unconditional.

    The reading feeds the canonical symbol table, which is built per process
    root, so a components-only apply paid platform calls for bytes no check
    reads. The saving the hoist earns is unaffected — that is a separate guard.
    """
    import ast
    from pathlib import Path

    module = Path(__file__).resolve().parents[1] / "src/boomi_mcp/categories/integration_builder.py"
    tree = ast.parse(module.read_text())
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
        and node.test.id == "process_units_by_key"
    ]
    assert len(guarded) == 1, "the account read is not gated on there being a consumer"



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

    unused = (
        '<?xml version="1.0"?><!DOCTYPE d [<!ENTITY unused "x">]>'
        '<Component subType="officialboomi-X3979C-rest-prod"><object>'
        '<operation customOperationType="GET"/>'
        '<field id="path" value="/x"/></object></Component>'
    )
    identity = live_identity_from_component_xml("op", unused)
    assert identity.route_state == "unavailable", (
        "an unreferenced entity declaration was accepted, so the refusal is "
        "keyed on expansion size rather than on the declaration"
    )

    # The CONTROL: the same document without the declaration reads normally, so
    # the refusal above is about the entity and not about the shape.
    without = (
        '<Component subType="officialboomi-X3979C-rest-prod"><object>'
        '<operation customOperationType="GET"/>'
        '<field id="path" value="/x"/></object></Component>'
    )
    control = live_identity_from_component_xml("op", without)
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
