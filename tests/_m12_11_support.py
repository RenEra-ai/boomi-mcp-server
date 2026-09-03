import pathlib
"""Shared fixtures for the #146 (M12.11) authoring-surface tests.

Not a ``test_*`` module, so pytest does not collect it. There is no
``conftest.py`` in this repo — every test file does its own ``sys.path`` insert —
so this module does the same and is imported by the eight ``test_m12_11_*``
files.

Imports use the bare ``boomi_mcp.`` prefix, matching every other M12 test.
"""

import sys
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    IntegrationSpecAuthoringIntentV1,
    ProcessIRAuthoringIntentV1,
)
from boomi_mcp.models.integration_models import (  # noqa: E402
    IntegrationComponentSpec,
    IntegrationSpecV1,
)
from boomi_mcp.models.process_component import (  # noqa: E402
    ProcessAuthoringUnitV1,
    ProcessComponentEnvelopeV1,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402

#: A minimal VALID connector flow: source -> message -> target -> stop.
#: Valid matters — a fixture the compiler rejects would make every "compiles
#: cleanly" assertion below vacuously true for the wrong reason.
VALID_IR_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "source",
                "connection_ref": "$ref:db_conn",
                "operation_ref": "$ref:db_op",
            },
            {"kind": "message", "text": "hello"},
            {
                "kind": "target",
                "connection_ref": "$ref:api_conn",
                "operation_ref": "$ref:api_op",
            },
            {"kind": "stop"},
        ],
    },
}

#: The same flow with the target's operation pointing at a ref no component
#: declares — a genuine semantic-validation failure, not a schema typo.
UNRESOLVABLE_IR_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {
                "kind": "source",
                "connection_ref": "$ref:db_conn",
                "operation_ref": "$ref:db_op",
            },
            {
                "kind": "target",
                "connection_ref": "$ref:api_conn",
                "operation_ref": "$ref:nonexistent_op",
            },
            {"kind": "stop"},
        ],
    },
}


def component(key, type_, name, **config):
    return IntegrationComponentSpec(key=key, type=type_, name=name, config=config)


def components(process_kind=None):
    """The fixture component plan.

    ``process_kind`` is what makes the plan MATERIALIZABLE. Without it the
    process can be planned and compiled (both read-only) but not applied — the
    builders emit process XML from ``config.process_kind``, and nothing on a
    production path materializes a ProcessIR root. Tests that exercise apply
    pass one; tests that exercise plan/compile do not.
    """
    process_config = {"process_kind": process_kind} if process_kind else {}
    return (
        IntegrationComponentSpec(
            # `action` is EXPLICIT: since §6 AR1-08 the recipe lift requires
            # both `name` and `action` caller-authored (`model_fields_set`),
            # never the model default.
            key="proc", type="process", name="M12.11 Process",
            action="create", config=process_config
        ),
        component(
            "db_conn", "connector-settings", "M12.11 DB Conn", connector_type="database"
        ),
        component(
            "db_op",
            "connector-operation",
            "M12.11 DB Op",
            connector_type="database",
            action_type="GET",
        ),
        component(
            "api_conn", "connector-settings", "M12.11 API Conn", connector_type="http"
        ),
        component(
            "api_op",
            "connector-operation",
            "M12.11 API Op",
            connector_type="http",
            action_type="SEND",
        ),
    )


def process_unit(key="proc", name="M12.11 Process", doc=None, **envelope_extra):
    """One ``ProcessAuthoringUnitV1`` over the fixture root (issue #153).

    ``name`` and ``action`` are REQUIRED on the direct authoring surface, so the
    helper supplies both rather than letting a default decide what gets created.
    """
    envelope_kwargs = {
        "component_key": key,
        "name": name,
        "action": "create",
        # #153: a root must DECLARE every `$ref` it uses. The fixture IR
        # references all four supporting components, so the envelope declares
        # them — ordered apply binds references from the id registry in
        # topological order, and an undeclared reference is one whose component
        # may not exist yet when the root is materialized.
        "depends_on": ("api_conn", "api_op", "db_conn", "db_op"),
    }
    envelope_kwargs.update(envelope_extra)
    return ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(**envelope_kwargs),
        process_ir=parse_process_ir_v1(doc or VALID_IR_DOC),
    )


def supporting_components(process_kind=None):
    """The fixture plan MINUS the process entry.

    Since #153 a direct ProcessIR intent carries its process as a UNIT, and
    `components[].key` shares one namespace with
    `processes[].envelope.component_key` — so leaving `proc` in both would be a
    duplicate key, not a convenience.
    """
    return tuple(c for c in components(process_kind) if c.key != "proc")


def process_ir_request(doc=None, process_kind=None, units=None, **extra):
    """A typed ProcessIR authoring request over the fixture component plan.

    ``process_kind`` is retained ONLY so callers that still pass it keep working
    on the supporting components; it no longer has any bearing on whether the
    process itself can be applied. That is the point of #153: a ProcessIR root is
    materialized from its own compiled artifact through the canonical chain, not
    by resolving a legacy dialect on a component config.
    """
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="M12.11 Integration",
            units=units if units is not None else (process_unit(doc=doc),),
            components=supporting_components(process_kind),
        ),
        **extra,
    )


#: An APPLIABLE supporting plan, deliberately REST-only.
#:
#: `components()` above carries a database connection the builder's own preflight
#: marks `error_database_validation`, which since #153 is a compile ERROR for
#: every intent kind (an unexecutable step means apply would refuse). So a test
#: whose subject is the canonical apply cannot use that plan — it never reaches
#: the canonical arm. Defined HERE rather than in the one test file that needed
#: it first, so there is a single fixture rather than two hand-copies drifting
#: apart.
APPLIABLE_CONN = {
    "key": "conn",
    "type": "connector-settings",
    "name": "M12.15 conn",
    "action": "create",
    "config": {
        "connector_type": "rest",
        "component_name": "M12.15 conn",
        "base_url": "https://orders.example.invalid",
        "auth": "NONE",
    },
}
APPLIABLE_OP = {
    "key": "op",
    "type": "connector-action",
    "name": "M12.15 op",
    "action": "create",
    "depends_on": ["conn"],
    "config": {
        "connector_type": "rest",
        "operation_mode": "execute",
        "component_name": "M12.15 op",
        "connection_ref_key": "conn",
        "method": "GET",
        "path": "/v1/things",
    },
}

#: source -> message -> return_documents over the two components above.
APPLIABLE_IR_DOC = {
    "version": "1",
    "body": {
        "kind": "sequence",
        "steps": [
            {"kind": "source", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
            {"kind": "message", "text": "hello"},
            {"kind": "return_documents"},
        ],
    },
}


def appliable_process_unit(key="proc", name="M12.15 Process", **envelope_extra):
    """One authoring unit over the appliable root."""
    kwargs = {
        "component_key": key,
        "name": name,
        "action": "create",
        "depends_on": ("conn", "op"),
    }
    kwargs.update(envelope_extra)
    return ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(**kwargs),
        process_ir=parse_process_ir_v1(APPLIABLE_IR_DOC),
    )


def appliable_process_ir_request(units=None, components=None):
    """A direct ProcessIR request the existing builders CAN plan, compile and apply."""
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="M12.15 Integration",
            units=units if units is not None else (appliable_process_unit(),),
            components=(
                components
                if components is not None
                else (APPLIABLE_CONN, APPLIABLE_OP)
            ),
        )
    )


def integration_spec_request(**extra):
    """A typed request wrapping an already-assembled component plan.

    Carries a process whose ProcessIR root is NOT derivable, which is the case
    this intent must report as a capability gap rather than silently pass.
    """
    return AuthoringRequestV1(
        intent=IntegrationSpecAuthoringIntentV1(
            integration_spec=IntegrationSpecV1(
                name="M12.11 Integration",
                components=[
                    component(
                        "proc",
                        "process",
                        "M12.11 Process",
                        process_kind="wrapper_subprocess",
                    )
                ],
            )
        ),
        **extra,
    )


def appliable_request(name="M12.11 Applied", **extra):
    """A typed request whose component plan the existing builders CAN materialize.

    Kept as the fixture for tests whose subject is the apply GATE itself — the
    binding, the phase order, the refusals — because a component-only plan keeps
    those tests focused on the gate rather than on materialization.

    The old reason for its existence is gone: a direct ``process_ir`` intent was
    plan/compile-only by design until #153, which is the capability that
    milestone ships. Tests whose subject IS the canonical apply live in
    ``test_issue_153_canonical_apply_e2e.py`` and drive a ``process_ir`` intent
    end to end.
    """
    return AuthoringRequestV1(
        intent=IntegrationSpecAuthoringIntentV1(
            integration_spec=IntegrationSpecV1(
                name=name,
                components=[
                    component(
                        "api_conn",
                        "connector-settings",
                        "M12.11 Applied Conn",
                        connector_type="http",
                        component_name="M12.11 Applied Conn",
                        url="https://api.example.com",
                    )
                ],
            )
        ),
        **extra,
    )


class MutationSpy:
    """Fails the moment any Boomi write helper is called.

    Patched over the builder's six component create/update helpers — which is
    every write door out of `integration_builder`. It does NOT patch
    `execute_process` or `manage_deployment`; the read-only orchestration never
    imports them, and `test_the_read_only_phase_never_imports_a_write_helper`
    asserts that structurally rather than by spying.

    "Plan performs zero remote mutation" is an acceptance criterion, and the only
    way to test a negative is to make the forbidden call explode.
    """

    #: Every write door out of the builder module, by name.
    TARGETS = (
        "create_component",
        "update_component",
        "create_connector",
        "update_connector",
        "create_trading_partner",
        "update_trading_partner",
    )

    def __init__(self):
        self.calls = []

    def install(self, monkeypatch):
        import boomi_mcp.categories.integration_builder as builder

        for name in self.TARGETS:
            if not hasattr(builder, name):  # pragma: no cover - defensive
                continue
            monkeypatch.setattr(
                builder, name, self._make(name), raising=True
            )
        return self

    def _make(self, name):
        def _explode(*args, **kwargs):
            self.calls.append(name)
            raise AssertionError(
                f"{name} was called during a read-only phase — this phase must "
                "perform zero remote mutation"
            )

        return _explode


def walk_strings(value, path="$"):
    """Yield ``(path, string)`` for every string anywhere in a JSON-ish value."""
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from walk_strings(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from walk_strings(item, f"{path}[{index}]")


def walk_keys(value, path="$"):
    """Yield ``(path, key)`` for every mapping key anywhere in a JSON-ish value."""
    if isinstance(value, dict):
        for key, item in value.items():
            yield path, key
            yield from walk_keys(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from walk_keys(item, f"{path}[{index}]")


#: The account the archived PATCH capture was taken in, and therefore the account
#: its operation record is scoped to. A witness for the evidenced path must run
#: as a client that REPORTS this account: the record names an account scope, and
#: a scope claim can only be satisfied by knowing which account you are in.
#:
#: Read from the capture's own derivation record rather than typed here, so a
#: re-capture in another account cannot leave this constant quietly wrong.
def capture_account_id():
    """The account id the archived operation record was minted against."""
    import json
    import pathlib

    derivation = (pathlib.Path(__file__).resolve().parents[1]
                  / "docs/architecture/evidence/issue-155/captures"
                  / ingested_operation_capture() / "record_derivation.json")
    return json.loads(derivation.read_text(encoding="utf-8"))[
        "account_scope_hash"]["account_id"]


def evidenced_account_client():
    """A fake client that REPORTS the capture's account, as the SDK client does.

    A bare `MagicMock()` answers every attribute with another Mock, so the
    account reader — which requires a non-empty `str` — finds none. Every witness
    for the evidenced path used one, which meant they all ran through the arm
    where a missing account skipped the scope check instead of failing it. The
    issue-level architect gate found that fail-open ordering, and these witnesses
    were the reason it stayed invisible: they proved the path worked in the one
    configuration where the check did not run.
    """
    from unittest.mock import MagicMock

    client = MagicMock()
    client._base_url_account_id = capture_account_id()
    client.account_id = capture_account_id()
    return client


def ingested_operation_capture() -> str:
    """The capture directory the PACKAGED registry was built from.

    Read from the ingest script's own constant rather than written here. Seven
    call sites named `cap155-e7-…` as a literal and went on passing through the
    `e8` re-capture purely because that capture's operation XML happened not to
    have changed — so the fixtures were serving a component the shipped record no
    longer described, and nothing said so. The `e9` re-capture moved the operation
    config digest and they all failed at once, which is the good outcome of a bad
    arrangement: a literal that is right by coincidence fails late and together.
    """
    import re

    text = (pathlib.Path(__file__).resolve().parents[1]
            / "scripts" / "ingest_connector_replay_evidence.py").read_text()
    found = re.search(r'OPERATION_RECORD_CAPTURES = \(\s*"([^"]+)"', text)
    assert found, "the ingest script no longer declares its capture set"
    return found.group(1)


def appliable_op_matching_the_capture() -> dict:
    """``APPLIABLE_OP`` with the route and verb the CAPTURED component stores.

    The declared config hard-codes a route, which was harmless while the
    declaration was never compared to the account. It is compared now, so a
    fixture that serves the captured operation's XML and declares a different
    route is asserting something false and is refused — correctly.

    Derived from the capture's own bytes rather than restated, so a re-capture
    that moves the stored route (a drift arm leaves one behind) carries the
    fixtures with it instead of stranding them on a value nothing serves.
    """
    import copy
    import re

    captures = (pathlib.Path(__file__).resolve().parents[1]
                / "docs/architecture/evidence/issue-155/captures"
                / ingested_operation_capture())
    xml = (captures / "component_op_tgt.xml").read_text(encoding="utf-8")
    op = copy.deepcopy(APPLIABLE_OP)
    path = re.search(r'<field id="path"[^>]*value="([^"]*)"', xml)
    if path:
        op["config"]["path"] = path.group(1)
    # THE VERB IS DELIBERATELY NOT OVERRIDDEN. The declared method drives which
    # capability the fixture is exercising, and the comparison that made the route
    # matter treats family and action as the caller's own assertion about a
    # component it names — an assertion these fixtures make on purpose and which
    # the surrounding tests are about. Only the ROUTE is taken from the account,
    # because only the route was hard-coded to a value nothing serves.
    return op
