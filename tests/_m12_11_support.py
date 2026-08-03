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
            key="proc", type="process", name="M12.11 Process", config=process_config
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


def process_ir_request(doc=None, process_kind=None, **extra):
    """A typed ProcessIR authoring request over the fixture component plan."""
    return AuthoringRequestV1(
        intent=ProcessIRAuthoringIntentV1(
            integration_name="M12.11 Integration",
            component_key="proc",
            process_ir=parse_process_ir_v1(doc or VALID_IR_DOC),
            components=components(process_kind),
        ),
        **extra,
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

    A direct ``process_ir`` intent is plan/compile-only by design — process
    materialization emits XML from the component plan, so applying one would
    create an artifact the compile hash does not describe. Tests that exercise
    the apply gate therefore use an ``integration_spec`` intent over components
    the legacy builders already know how to create.
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

    Patched over the builder's create/update helpers AND the execute/deploy
    actions. "Plan performs zero remote mutation" is an acceptance criterion, and
    the only way to test a negative is to make the forbidden call explode.
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
