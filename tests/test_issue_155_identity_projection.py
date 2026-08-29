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
