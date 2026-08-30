"""No module may re-spell an identifier language `connector_replay.ids` owns.

This is the structural half of the hand-copy fix. Repointing the two known sites
proves nothing on its own: the next author to need a component-id check will
write the regex out again, and the copy will pass every test in the suite while
it agrees with the original — right up until one of them changes.

The guard derives its own universe by PARSING the tree. It does not carry a list
of files to check, because a hand-maintained list of the places a hand-copy might
appear is the same defect one level up: the list would need updating by exactly
the author who did not know the rule existed.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
_OWNER = _SRC / "boomi_mcp" / "connector_replay" / "ids.py"

#: The shape being protected: a canonical 8-4-4-4-12 hex run, however its
#: character classes are spelled. Matching the SHAPE rather than an exact string
#: is the point — the two copies this rule was written for differed textually
#: (`[0-9a-f]` plus IGNORECASE versus `[0-9a-fA-F]`) while describing one
#: language, so a guard keyed on exact text would have found neither.
_HEX_UUID_SHAPE = re.compile(
    r"\[0-9a-f(?:A-F)?\]\{8\}-"
    r"\[0-9a-f(?:A-F)?\]\{4\}-"
    r"\[0-9a-f(?:A-F)?\]\{4\}-"
    r"\[0-9a-f(?:A-F)?\]\{4\}-"
    r"\[0-9a-f(?:A-F)?\]\{12\}",
    re.IGNORECASE,
)


def _string_literals(tree: ast.AST) -> list[str]:
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _modules_spelling_the_shape() -> list[str]:
    offenders: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover - a broken file is another test's problem
            continue
        if any(_HEX_UUID_SHAPE.search(s) for s in _string_literals(tree)):
            offenders.append(str(path.relative_to(_SRC)))
    return offenders


def test_exactly_one_module_spells_the_component_id_language():
    offenders = _modules_spelling_the_shape()
    expected = [str(_OWNER.relative_to(_SRC))]
    assert offenders == expected, (
        "the component-id language must be spelled in exactly one module. "
        "Found it in {0}; expected only {1}. Import BOOMI_COMPONENT_ID_RE from "
        "boomi_mcp.connector_replay.ids instead of writing the pattern again."
        .format(offenders, expected)
    )


def test_the_guard_is_not_vacuous():
    """Prove the detector actually fires — on a case it is meant to catch.

    A guard that scans for a shape nothing matches passes forever and protects
    nothing. This runs the detector over BOTH spellings the real copies used,
    plus a control that must not match.
    """
    lowercase_with_flag = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    explicit_both_cases = (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    assert _HEX_UUID_SHAPE.search(lowercase_with_flag)
    assert _HEX_UUID_SHAPE.search(explicit_both_cases)
    # a control: a similar-looking but different language must NOT match
    assert not _HEX_UUID_SHAPE.search(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}$")


def test_the_owner_module_really_is_where_the_shape_lives():
    """Anchors the expectation above to a file that exists and spells it."""
    assert _OWNER.is_file()
    assert any(_HEX_UUID_SHAPE.search(s) for s in _string_literals(ast.parse(_OWNER.read_text())))


def test_the_registry_layer_loads_without_the_authoring_stack():
    """`connector_replay` must be importable on its own.

    The registry is meant to be packaged and read by tooling — including from
    inside a built image — without dragging the compiler or the MCP category
    modules in behind it. This is a load-time property, so it is checked by
    importing into a cleaned module table rather than by reading the source for
    import statements: a lazily-imported name inside a function is fine, and a
    grep-based check would flag it while missing a transitive pull-in.
    """
    import subprocess
    import sys
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]
    code = (
        "import sys\n"
        "import importlib, pkgutil\n"
        "import boomi_mcp.connector_replay as pkg\n"
        # Import EVERY submodule, discovered rather than listed: a module added
        # later would otherwise never be checked, and the one that breaks this
        # property is always the one nobody remembered to add here.
        "for m in pkgutil.iter_modules(pkg.__path__):\n"
        "    importlib.import_module('boomi_mcp.connector_replay.' + m.name)\n"
        "leaked = sorted(k for k in sys.modules if k.startswith('boomi_mcp.categories')"
        " or k.startswith('boomi_mcp.compiler'))\n"
        "print(';'.join(leaked))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=repo, capture_output=True, text=True,
        env={"PYTHONPATH": str(repo / "src"), "PATH": "/usr/bin:/bin",
             "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert out.returncode == 0, out.stderr
    leaked = [x for x in out.stdout.strip().split(";") if x]
    assert leaked == [], (
        "importing the replay registry pulled in the authoring stack: {0}. Move the "
        "offending import inside the function that needs it.".format(leaked)
    )


def test_every_registered_replay_code_has_a_raiser():
    """A published code nothing can produce is a promise the system cannot keep.

    This repository already enforces the property for compiler diagnostics. The
    reasoning does not depend on the code being a compiler diagnostic: a caller
    that branches on a code which is never raised has written dead error handling,
    and one reading the taxonomy as documentation is told about a condition that
    cannot occur.

    The universe is derived from the taxonomy itself, so a code added to the family
    later is covered without editing this test.
    """
    import pytest

    from boomi_mcp.connector_replay.digests import (
        ConfigDigestRefused,
        RouteDigestRefused,
        component_config_digest_v1,
        route_digest_v1,
    )
    from boomi_mcp.connector_replay.registry import RegistryInvalid, _parse
    from boomi_mcp.errors import ERROR_TAXONOMY

    registered = {
        code for code, spec in ERROR_TAXONOMY.items()
        if code.startswith("CONNECTOR_REPLAY_")
    }
    assert registered, "the family is empty; this check would be vacuous"

    produced: set[str] = set()

    with pytest.raises(RouteDigestRefused) as route_err:
        route_digest_v1("<C/>", "<O/>")
    produced.add(route_err.value.code)

    with pytest.raises(ConfigDigestRefused) as cfg_err:
        component_config_digest_v1("<C/>", "not-a-kind")
    produced.add(cfg_err.value.code)

    with pytest.raises(RegistryInvalid) as reg_err:
        _parse({"schema_version": 999})
    produced.add(reg_err.value.code)

    # Slice C's two identity codes are raised in the AUTHORING layer, never under
    # compiler/process_ir — a code named there joins the compiler's published
    # surface, and connector identity is an account fact. Both are produced from
    # real inputs rather than constructed, so this stays a raiser check.
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        ConnectorIdentityError,
        TrustedConnectorResolutionSnapshotV1,
        ResolvedConnectorComponentIdentityV1,
        assert_declared_matches_resolved,
    )

    snapshot = TrustedConnectorResolutionSnapshotV1(
        identities=(
            ResolvedConnectorComponentIdentityV1(
                component_key="rest_conn",
                family="rest",
                action="POST",
                endpoint="http://host:8081",
                route_state="static",
            ),
        )
    )
    with pytest.raises(ConnectorIdentityError) as mismatch:
        assert_declared_matches_resolved(snapshot, {"rest_conn": ("rest", "GET")})
    produced.add(mismatch.value.code)

    # The unreadable-payload refusal, raised from the same layer and from a real
    # component rather than a constructed identity: the caller supplies raw
    # component XML behind a document-type declaration, which the reader will not
    # parse, and the caller's own bytes fail closed rather than falling silent.
    from boomi_mcp.authoring.connector_resolution_snapshot import (
        build_connector_resolution_snapshot,
    )
    from boomi_mcp.models.integration_models import IntegrationComponentSpec

    with pytest.raises(ConnectorIdentityError) as unreadable:
        build_connector_resolution_snapshot([
            IntegrationComponentSpec(
                key="raw",
                type="connector-action",
                action="create",
                config={
                    "connector_type": "rest_client",
                    "method": "GET",
                    "xml": '<!DOCTYPE Component [<!ENTITY x "y">]><Component/>',
                },
            )
        ])
    produced.add(unreadable.value.code)

    # The reuse the account could not answer for. Raised from a real component
    # and a real reading rather than constructed, so this stays a raiser check.
    with pytest.raises(ConnectorIdentityError) as unavailable:
        build_connector_resolution_snapshot(
            [
                IntegrationComponentSpec(
                    key="reused",
                    type="connector-action",
                    action="create",
                    component_id="id-1",
                    config={"connector_type": "rest_client"},
                )
            ],
            live_component_xml={"reused": {"xml": None, "read_failed": True}},
            reused_keys={"reused"},
        )
    produced.add(unavailable.value.code)

    # Slice D's discovery code is PRODUCED IN A SERVED ENVELOPE, not raised. The
    # property this test defends is that a published code can actually occur, and
    # a tool action reports by returning `{"_success": False, "error_code": ...}` —
    # a caller branches on that exactly as it would on a raised one. Exercised for
    # real: an identity the account cannot read.
    from boomi_mcp.connector_replay.discovery import idempotency_contract_candidates

    class _EmptyRegistry:
        operation_records = ()

    unavailable_discovery = idempotency_contract_candidates(
        operation_component_id="op-1",
        connection_component_id="cn-1",
        live_identity=lambda component_id: None,
        registry=_EmptyRegistry(),
    )
    assert unavailable_discovery["_success"] is False
    produced.add(unavailable_discovery["error_code"])

    assert registered == produced, {
        "registered but never produced": sorted(registered - produced),
        "produced but not registered": sorted(produced - registered),
    }
