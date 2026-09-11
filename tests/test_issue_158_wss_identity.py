"""#158: a native Web Services Server operation resolves as a listener's operation.

A WSS listener operation is authored the way its builder takes it —
``operation_mode="listen"`` and no ``action_type`` — and must resolve to the
``Listen`` action on every rung that decides a connector's identity: the
authoring intake (``_action_type_from_config``), the pre-apply identity
projection, the #155 resolution snapshot (structured, submitted-XML and
live-readback rungs), the symbol table the compiler receives, and the listener
entry's own reference/inbound checks. These tests pin each rung and the
parity between them, then drive one listener root through the public
``build_integration`` plan and compile route.

Fixture provenance (causally independent of the code under test):

* **Operation XML** is produced by CALLING ``WssListenerOperationBuilder.build``.
  Its ``WebServicesServerListenAction`` element reproduces the live capture of
  renera op 601cf5a3 ("Configure a Web Listener", 2026-07-04) and is locked
  against that capture by ``tests/test_wss_listener_operation_builder.py``. The
  base config below is that capture's own configuration. No listener element is
  hand-typed here.
* **Live-readback envelope**: the XML prolog, root-element attributes and the
  trailing ``bns:GlobalVariableReferences`` of a real platform GET — the archived
  capture ``docs/architecture/evidence/issue-155/captures/cap155-e2-post/
  operation_component.xml`` — with the builder's ``bns:object`` spliced in and
  the subtype set to ``wss``. So the reader sees the envelope the platform
  actually serves, around the body the platform actually stores.
* **Refusal documents** are single mutations of the builder's XML (the Listen
  element removed, relocated, wrapped, or replaced by another family's element),
  never invented documents.
* **Vocabularies** (aliases, operation types, the default input type, the
  profile-bindable input types) are read from the builder's own constants and
  pinned bidirectionally against the lists this file states.
* **Outbound REST fixtures** are the M12.15 appliable components in
  ``tests/_m12_11_support.py``; symbol ids are the placeholder resolver's.
"""

from __future__ import annotations

import copy
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import APPLIABLE_CONN, APPLIABLE_OP  # noqa: E402
from boomi_mcp.authoring import connector_resolution_snapshot as snapshot_module  # noqa: E402
from boomi_mcp.authoring.connector_resolution_snapshot import (  # noqa: E402
    ConnectorIdentityError,
    assert_declared_matches_resolved,
    build_connector_resolution_snapshot,
    live_identity_from_component_xml,
)
from boomi_mcp.authoring.workflow import (  # noqa: E402
    _action_type_from_config,
    _connector_metadata_from_components,
)
from boomi_mcp.categories import integration_builder  # noqa: E402
from boomi_mcp.categories.components.builders import connector_builder  # noqa: E402
from boomi_mcp.categories.components.builders.connector_builder import (  # noqa: E402
    SET_BY_EXTENSION,
    WssListenerOperationBuilder,
    _WSS_ALIASES,
    _WSS_JSON_XML_TYPES,
    _WSS_OPERATION_TYPES,
    _normalized_action,
    connector_family_of,
    get_connector_action_builder,
    normalized_identity_projection,
    wss_listen_action_from_config,
    wss_listener_inbound_facts,
)
from boomi_mcp.compiler.process_ir.connector_resolution import (  # noqa: E402
    LISTENER_ACTION,
    is_listener_operation_symbol,
    profile_bound_input_types,
)
from boomi_mcp.compiler.process_ir.contracts import LISTENER_CONNECTOR_TYPES  # noqa: E402
from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.errors import (  # noqa: E402
    CONNECTOR_REPLAY_IDENTITY_MISMATCH,
    CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE,
    PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID,
    PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringRequestV1,
    IntegrationSpecAuthoringIntentV1,
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
from boomi_mcp.recipes.materialization import build_symbol_table  # noqa: E402

_PROFILE = "qa_profile"
_PAGINATE = "boomi_mcp.categories.integration_builder.paginate_metadata"

#: The spellings this file claims are the WSS family, pinned against the builder.
_ALIASES = ("wss", "web_services", "web_services_server")
#: The served-verb vocabulary of a WSS operation (`operationType`), pinned too.
_OPERATION_TYPES = ("CREATE", "EXECUTE", "GET", "QUERY", "UPDATE", "UPSERT", "DELETE")
#: Spellings in the compiler's listener REFUSAL set that no builder accepts.
_REFUSAL_ONLY_SPELLINGS = ("wssserver", "listener")

#: The archived platform GET whose envelope wraps the builder's body.
_READBACK_CAPTURE = (
    Path(__file__).resolve().parents[1]
    / "docs/architecture/evidence/issue-155/captures/cap155-e2-post/operation_component.xml"
)

#: A literal profile component id: outside any symbol table, so it cannot be
#: classified offline and is accepted as the listener builder's contract accepts it.
_LITERAL_PROFILE_ID = "5f3c3a2e-1111-4222-8333-444444444444"


@pytest.fixture(autouse=True)
def _clean_registry():
    integration_builder._BUILD_REGISTRY.clear()
    yield
    integration_builder._BUILD_REGISTRY.clear()


def test_the_pinned_vocabularies_are_the_builders_own():
    """Bidirectional pins: this file's lists ARE the authorities', not copies."""
    assert tuple(_WSS_ALIASES) == _ALIASES
    assert frozenset(_OPERATION_TYPES) == _WSS_OPERATION_TYPES
    assert profile_bound_input_types() == _WSS_JSON_XML_TYPES
    assert WssListenerOperationBuilder.SUPPORTED_OPERATION_MODES == ("listen",)
    assert frozenset(_REFUSAL_ONLY_SPELLINGS) == (
        LISTENER_CONNECTOR_TYPES - frozenset(_WSS_ALIASES)
    )
    for spelling in _REFUSAL_ONLY_SPELLINGS:
        assert connector_family_of(spelling) is None, spelling


# ---------------------------------------------------------------------------
# fixture builders
# ---------------------------------------------------------------------------


def _wss_config(**overrides):
    """The live-captured listener operation's configuration (op 601cf5a3).

    Authored natively: ``operation_mode="listen"`` and NO ``action_type``.
    """
    config = {
        "connector_type": "wss",
        "operation_mode": "listen",
        "component_name": "Configure a Web Listener",
        "object_name": "generalListener",
        "operation_type": "CREATE",
        "output_type": "none",
        "response_content_type": "text/plain",
    }
    config.update(overrides)
    return config


def _built(config):
    """The operation XML the WSS builder emits for ``config``."""
    params = {k: v for k, v in config.items() if k != "connector_type"}
    return WssListenerOperationBuilder().build(**params)


def _platform_readback(builder_xml, component_id="0f5a9d7e-1580-4b1c-9e58-158158158158"):
    """``builder_xml`` in the envelope a platform GET returns.

    The prolog, root attributes and trailing children come from the archived
    REST GET capture; only ``componentId`` and ``subType`` are re-stamped, and
    the ``bns:object`` is the builder's.
    """
    assert _READBACK_CAPTURE.is_file(), "the archived platform GET capture is missing"
    capture = _READBACK_CAPTURE.read_text()
    root_start = capture.index("<bns:Component")
    prolog = capture[:root_start]
    root = re.match(r"<bns:Component\b[^>]*>", capture[root_start:]).group(0)
    root = re.sub(r'\scomponentId="[^"]*"', f' componentId="{component_id}"', root)
    root = re.sub(r'\ssubType="[^"]*"', ' subType="wss"', root)
    assert 'type="connector-action"' in root and 'subType="wss"' in root, root
    tail = capture[capture.index("</bns:object>") + len("</bns:object>"):]
    body = re.search(r"<bns:object>.*</bns:object>", builder_xml, re.S).group(0)
    return (
        prolog + root + "<bns:encryptedValues/><bns:description></bns:description>"
        + body + tail
    )


def _spec(key, type_, config, name=None):
    return IntegrationComponentSpec(
        key=key, type=type_, name=name or key, action="create", config=config
    )


def _rest_components():
    """The appliable REST connection and a POST operation bound to it."""
    conn = IntegrationComponentSpec(**APPLIABLE_CONN)
    op_payload = copy.deepcopy(APPLIABLE_OP)
    op_payload["config"]["method"] = "POST"
    return conn, IntegrationComponentSpec(**op_payload)


def _listener_doc(operation_ref="$ref:wss_op", inbound_validation=None):
    listener = {"kind": "listener", "operation_ref": operation_ref}
    if inbound_validation is not None:
        listener["inbound_validation"] = inbound_validation
    return {"version": "1", "body": {"kind": "sequence", "steps": [
        listener,
        {"kind": "target", "connection_ref": "$ref:conn", "operation_ref": "$ref:op"},
        {"kind": "stop"},
    ]}}


def _table(components, *, snapshot="build", **snapshot_kwargs):
    """The symbol table the authoring route builds, from real component specs."""
    metadata = _connector_metadata_from_components(components)
    if snapshot == "build":
        snapshot = build_connector_resolution_snapshot(
            components, declared=metadata, **snapshot_kwargs
        )
    return build_symbol_table(
        components, connector_metadata=metadata, connector_resolution_snapshot=snapshot
    )


def _symbol(table, key):
    return table.build_index()["$ref:" + key]


def _compile_codes(doc, table):
    """``None`` when the root compiles, else the diagnostic ``(code, path)`` pairs."""
    try:
        compile_process_ir_v1(parse_process_ir_v1(doc), table)
    except ProcessIRCompileError as exc:
        return [(d.code, d.path) for d in exc.diagnostics]
    return None


# ---------------------------------------------------------------------------
# the intake derivation
# ---------------------------------------------------------------------------


def _spelling_variants(alias):
    return (alias, alias.upper(), alias.title(), f"  {alias}  ", f"\t{alias.upper()}\n")


def test_native_listen_config_derives_listen_through_intake():
    """A native WSS config derives ``Listen`` with no ``action_type`` authored."""
    wrong = []
    for alias in _ALIASES:
        for spelling in _spelling_variants(alias):
            for mode in ("listen", "LISTEN"):
                config = {"connector_type": spelling, "operation_mode": mode}
                if _action_type_from_config(config) != "Listen":
                    wrong.append((config, _action_type_from_config(config)))
            # The WSS builder exposes ONE action; `execute` is not it.
            config = {"connector_type": spelling, "operation_mode": "execute"}
            if _action_type_from_config(config) is not None:
                wrong.append((config, _action_type_from_config(config)))
    assert wrong == [], wrong

    # The derivation, not an authored alias, decides a native listen config: a
    # contradictory `action_type` beside `operation_mode="listen"` does not win.
    assert _action_type_from_config(
        {"connector_type": "wss", "operation_mode": "listen", "action_type": "Get"}
    ) == "Listen"

    # `listen` on any other family derives nothing — including the listener
    # refusal-only spellings no builder accepts, and the outbound SOAP client
    # that must never be misrouted to the inbound listener.
    for connector_type in (
        "soap_client", "web_services_soap_client", "soap", "rest", "rest_client",
        "database", "http", *_REFUSAL_ONLY_SPELLINGS, "", None,
    ):
        config = {"connector_type": connector_type, "operation_mode": "listen"}
        assert _action_type_from_config(config) is None, config


def test_intake_and_projection_share_one_listen_derivation():
    """Parity pin: intake, projection and the shared derivation agree on WSS.

    Scope, stated rather than implied: configs that author no ``action_type``.
    The alias fallback is intake-only for EVERY family (REST and SOAP included,
    pinned by ``test_the_action_type_derivation_matches_the_legacy_builder``);
    what this pins is that the native structured fields — the connector type and
    ``operation_mode`` — are read by ONE derivation on both sides.
    """
    modes = ("listen", "LISTEN", "Listen", " listen", "listen ", "execute", "get",
             "", None, 7, ["listen"])
    junk = ({}, {"method": "POST"}, {"base_url": "https://x.invalid", "path": "/in"},
            {"input_type": "singlexml", "request_profile": "$ref:p"})
    grid = [
        {"connector_type": spelling, "operation_mode": mode, **extra}
        for alias in _ALIASES
        for spelling in _spelling_variants(alias)
        for mode in modes
        for extra in junk
    ]
    disagreements = []
    answers = set()
    for config in grid:
        shared = wss_listen_action_from_config(config)
        intake = _action_type_from_config(config)
        projected = normalized_identity_projection(config).action
        mirrored = _normalized_action(config, connector_family_of(config["connector_type"]))
        answers.add(shared)
        if not (intake == projected == mirrored == shared):
            disagreements.append((config, intake, projected, mirrored, shared))
    assert disagreements == [], disagreements[:5]
    # Non-degenerate: the grid exercises both answers.
    assert answers == {"Listen", None}, answers

    # THE BUILDER DECIDES WHICH MODES ARE A LISTEN. The shared derivation answers
    # `Listen` for exactly the modes the WSS builder's own validator accepts and
    # its dispatch routes — so a normalisation drift in either direction (a
    # stripped or case-sensitive mode) derives an action for an operation that
    # cannot be built, or none for one that can.
    for mode in modes:
        config = _wss_config(operation_mode=mode)
        accepted = WssListenerOperationBuilder.validate_config(config) is None
        assert (wss_listen_action_from_config(config) == "Listen") is accepted, mode
        if isinstance(mode, str):
            routed = get_connector_action_builder("wss", mode)
            assert isinstance(routed, WssListenerOperationBuilder) is accepted, mode

    # Off the WSS family the shared derivation says nothing, and neither side
    # derives `Listen` from a listen mode.
    for connector_type in ("soap_client", "rest", "database", "http",
                           *_REFUSAL_ONLY_SPELLINGS):
        config = {"connector_type": connector_type, "operation_mode": "listen"}
        assert wss_listen_action_from_config(config) is None, config
        assert _action_type_from_config(config) != "Listen", config
        assert normalized_identity_projection(config).action != "Listen", config

    # Non-mapping input is refused by the derivation rather than raising.
    for junk_config in (None, "wss", 7, ["connector_type"]):
        assert wss_listen_action_from_config(junk_config) is None


def test_wss_identity_is_never_mintable():
    """A listener's endpoint is SERVED, not called — there is no route to pin."""
    route_junk = (
        {},
        {"base_url": "https://orders.example.invalid", "path": "/v1/orders"},
        {"base_url": "https://orders.example.invalid", "path": "/v1/{id}",
         "path_replacements": [{"name": "id", "target_path": "orderId"}]},
        {"endpoint_url": "https://soap.example.invalid/svc", "url": "https://u.invalid"},
        {"host": "db.example.invalid", "port": 1433, "dbname": "x"},
        {"base_url": SET_BY_EXTENSION},
    )
    for alias in _ALIASES:
        for spelling in _spelling_variants(alias):
            for extra in route_junk:
                config = {"connector_type": spelling, "operation_mode": "listen", **extra}
                for live in (None, {"url": "https://account.example.invalid/ws"}):
                    identity = normalized_identity_projection(config, live_projection=live)
                    seen = (identity.family, identity.action, identity.route_state,
                            identity.endpoint, identity.path, identity.mintable)
                    assert seen == ("wss", "Listen", "unavailable", None, None, False), (
                        config, live, seen,
                    )

    # A WSS config that does not listen settles no action and is no more mintable.
    execute = normalized_identity_projection(
        {"connector_type": "wss", "operation_mode": "execute",
         "base_url": "https://orders.example.invalid", "path": "/v1/orders"}
    )
    assert (execute.family, execute.action, execute.mintable) == ("wss", None, False)

    # NON-VACUITY: the same route fields on a REST config DO mint, so the WSS arm
    # — not an unusable route — is what refuses above.
    rest = normalized_identity_projection(
        {"connector_type": "rest_client", "method": "POST",
         "base_url": "https://orders.example.invalid", "path": "/v1/orders"}
    )
    assert (rest.family, rest.route_state, rest.mintable) == ("rest", "static", True)


# ---------------------------------------------------------------------------
# the snapshot rungs
# ---------------------------------------------------------------------------


def test_live_readback_resolves_listen_from_the_element_not_operation_type():
    """The element's PRESENCE is the verb; ``operationType`` is the served verb."""
    for operation_type in _OPERATION_TYPES:
        for request_profile in (None, "$ref:order_request_profile", _LITERAL_PROFILE_ID):
            overrides = {"operation_type": operation_type, "input_type": "singlejson"}
            if request_profile is not None:
                overrides["request_profile"] = request_profile
            config = _wss_config(**overrides)
            identity = live_identity_from_component_xml(
                "wss_op", _platform_readback(_built(config))
            )
            case = (operation_type, request_profile)
            assert identity.readable and identity.document_parsed is True, case
            assert identity.document_component_type == "connector-action", case
            assert (identity.family, identity.action) == ("wss", "LISTEN"), case
            assert identity.action.casefold() == LISTENER_ACTION.casefold(), case
            assert identity.action.casefold() == wss_listen_action_from_config(
                config
            ).casefold(), case
            assert (identity.source, identity.authority) == ("live", "live_readback_xml")
            assert identity.action_contradicted is False, case
            assert identity.listener_input_type == "singlejson", case
            assert identity.listener_request_profile == request_profile, case

    # Every input type the builder can emit is carried as the element states it.
    for input_type in sorted({"none", "singledata"} | set(_WSS_JSON_XML_TYPES)):
        identity = live_identity_from_component_xml(
            "wss_op", _platform_readback(_built(_wss_config(input_type=input_type)))
        )
        assert (identity.listener_input_type, identity.listener_request_profile) == (
            input_type, None,
        ), input_type

    # CONTROL: the same envelope around the platform's REST body still reads the
    # REST verb, so the reader discriminates by family rather than always
    # answering LISTEN.
    rest = live_identity_from_component_xml("op", _READBACK_CAPTURE.read_text())
    assert (rest.family, rest.action, rest.listener_input_type) == ("rest", "POST", None)

    # The declared-vs-account comparison folds, so a native declaration agrees
    # with the account's LISTEN — and a different declared verb is a mismatch.
    component = _spec("wss_op", "connector-action", _wss_config(input_type="singlejson"))
    component = component.model_copy(
        update={"component_id": "0f5a9d7e-1580-4b1c-9e58-158158158158"}
    )
    live_xml = _platform_readback(_built(_wss_config(input_type="singlejson")))
    snapshot = build_connector_resolution_snapshot(
        [component], live_component_xml={"wss_op": live_xml}, reused_keys={"wss_op"}
    )
    declared = _connector_metadata_from_components([component])
    assert declared == {"wss_op": ("wss", "Listen")}
    assert assert_declared_matches_resolved(snapshot, declared) == declared
    with pytest.raises(ConnectorIdentityError) as mismatch:
        assert_declared_matches_resolved(snapshot, {"wss_op": ("wss", "Get")})
    assert mismatch.value.code == CONNECTOR_REPLAY_IDENTITY_MISMATCH
    assert mismatch.value.field == "action"


def _without_listen_element(xml):
    """Five single mutations of the builder's document, none naming a Listen verb."""
    element = re.search(r"\s*<WebServicesServerListenAction [^>]*/>", xml).group(0)
    stripped = xml.replace(element, "")
    another_family = (
        '<GenericOperationConfig customOperationType="POST" operationType="EXECUTE"/>'
    )
    return {
        "empty Configuration": stripped,
        "Listen element beside Configuration, not in it": stripped.replace(
            "<Configuration>", element.strip() + "<Configuration>"
        ),
        "Listen element nested below a wrapper": xml.replace(
            element, "<Wrapper>" + element.strip() + "</Wrapper>"
        ),
        "another family's operation element": xml.replace(element, another_family),
        # `object/Decoy/Operation/Configuration/<Listen>`: the Operation/
        # Configuration pair, but not anchored under the component's object.
        "Listen element under a look-alike path": stripped.replace(
            "<bns:object>", "<bns:object><Decoy><Operation><Configuration>"
            + element.strip() + "</Configuration></Operation></Decoy>"
        ),
    }


def test_submitted_wss_xml_without_listen_element_is_refused():
    """Known-verb family, no action in the caller's own bytes: refused, not skipped."""
    config = _wss_config(input_type="singlejson", request_profile="$ref:order_request_profile")
    builder_xml = _built(config)

    # CONTROL FIRST: the builder's own XML, submitted raw, is accepted and
    # resolves Listen with the inbound facts it states.
    accepted = build_connector_resolution_snapshot(
        [_spec("raw", "connector-action", {**config, "xml": builder_xml})]
    ).lookup("raw")
    assert (accepted.family, accepted.action, accepted.authority) == (
        "wss", "LISTEN", "submitted_xml",
    )
    assert (accepted.listener_input_type, accepted.listener_request_profile) == (
        "singlejson", "$ref:order_request_profile",
    )

    variants = _without_listen_element(builder_xml)
    assert len(variants) == 5
    for label, document in variants.items():
        # Each variant is well-formed and classified as WSS: the refusal below is
        # the known-verb-family branch, not the unparseable-document branch.
        read = live_identity_from_component_xml("raw", document)
        assert read.readable and read.family == "wss", label
        assert read.action is None and read.action_contradicted is False, label

        component = _spec("raw", "connector-action", {**config, "xml": document})
        with pytest.raises(ConnectorIdentityError) as refused:
            build_connector_resolution_snapshot([component])
        assert refused.value.code == CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE, label
        assert "names no operation type" in str(refused.value), label
        assert refused.value.component_key == "raw", label

    # ...and the served surface carries the same code (the shared construction
    # plus the failure mapping a caller actually sees).
    class _Spec:
        components = [_spec("raw", "connector-action",
                            {**config, "xml": variants["empty Configuration"]})]
        processes = ()

    with pytest.raises(ConnectorIdentityError) as raised:
        integration_builder._build_canonical_symbols(
            spec=_Spec(), resolution=integration_builder._request_only_resolution(_Spec())
        )
    served, _path = integration_builder._canonical_plan_failure(raised.value)
    assert served == CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE


def test_the_submitted_xml_refusal_is_caused_by_the_wss_verb_location(monkeypatch):
    """Mutation witness: with ``wss`` outside the known-verb families, the same
    action-less document is accepted — so the refusal above is #158's, not
    something that fired on every document."""
    config = _wss_config()
    document = _without_listen_element(_built(config))["empty Configuration"]
    component = _spec("raw", "connector-action", {**config, "xml": document})
    monkeypatch.setattr(
        snapshot_module,
        "_FAMILIES_WITH_A_KNOWN_VERB_LOCATION",
        frozenset({"rest", "soap_client", "database"}),
    )
    identity = build_connector_resolution_snapshot([component]).lookup("raw")
    assert (identity.family, identity.action) == ("wss", None)


def test_structured_rung_carries_listener_inbound_facts():
    """The structured rung reads the inbound facts the way the builder EMITS them."""
    grid = [
        _wss_config(),                                             # builder default
        _wss_config(input_type="SingleXML"),
        _wss_config(input_type="none"),
        _wss_config(input_type="singledata"),
        _wss_config(input_type="multijson", request_profile="  $ref:order_request_profile  "),
        _wss_config(input_type="singlexml", request_profile=_LITERAL_PROFILE_ID),
        _wss_config(input_type="singlejson", request_profile=""),  # blank binds nothing
        _wss_config(connector_type="Web_Services_Server", input_type="multixml",
                    request_profile="$ref:order_request_profile"),
    ]
    for config in grid:
        identity = build_connector_resolution_snapshot(
            [_spec("wss_op", "connector-action", config)]
        ).lookup("wss_op")
        facts = (identity.listener_input_type, identity.listener_request_profile)
        assert identity.authority == "normalized_structured_fields", config
        assert (identity.family, identity.action, identity.route_state) == (
            "wss", "Listen", "unavailable",
        ), config
        assert facts == wss_listener_inbound_facts(config), config
        # PARITY WITH THE BYTES: what the structured rung reports is exactly what
        # the operation the builder emits for this config will store.
        emitted = live_identity_from_component_xml("wss_op", _built(config))
        assert facts == (
            emitted.listener_input_type, emitted.listener_request_profile,
        ), config

    default = build_connector_resolution_snapshot(
        [_spec("wss_op", "connector-action", _wss_config())]
    ).lookup("wss_op")
    assert default.listener_input_type == WssListenerOperationBuilder.DEFAULT_INPUT_TYPE
    assert default.listener_request_profile is None

    # Only a WSS listener config carries the facts: a WSS config that does not
    # listen, and an outbound REST operation, carry neither.
    conn, rest_op = _rest_components()
    not_listening = _spec("wss_exec", "connector-action",
                          _wss_config(operation_mode="execute", input_type="singlexml"))
    snapshot = build_connector_resolution_snapshot([conn, rest_op, not_listening])
    for key in ("conn", "op", "wss_exec"):
        identity = snapshot.lookup(key)
        assert (identity.listener_input_type, identity.listener_request_profile) == (
            None, None,
        ), key


# ---------------------------------------------------------------------------
# the symbol table
# ---------------------------------------------------------------------------


def test_symbol_table_carries_inbound_facts_and_no_connection():
    """The listener operation's symbol: Listen, inbound facts, no connection."""
    conn, rest_op = _rest_components()
    config = _wss_config(input_type="singlejson", request_profile="$ref:cfg_profile")
    wss_op = _spec("wss_op", "connector-action", config)
    components = [conn, rest_op, wss_op]

    # With no snapshot, and with a snapshot that does not name the operation, the
    # facts come from the operation's own config.
    for snapshot in (None, build_connector_resolution_snapshot([])):
        table = _table(components, snapshot=snapshot)
        symbol = _symbol(table, "wss_op")
        assert (symbol.input_document_type, symbol.input_profile_ref) == (
            "singlejson", "$ref:cfg_profile",
        )
        assert symbol.connection_ref is None
        assert symbol.action_type == "Listen"
        assert is_listener_operation_symbol(symbol)

    # The structured rung agrees with the config (no precedence question yet).
    structured = _symbol(_table(components), "wss_op")
    assert (structured.input_document_type, structured.input_profile_ref) == (
        "singlejson", "$ref:cfg_profile",
    )

    # PRECEDENCE: a snapshot whose facts DIFFER from the config wins — the
    # account's stored bytes for a reused operation...
    live_xml = _platform_readback(_built(_wss_config(
        input_type="singlexml", request_profile=_LITERAL_PROFILE_ID,
    )))
    reused = _symbol(
        _table(components, live_component_xml={"wss_op": live_xml},
               reused_keys={"wss_op"}),
        "wss_op",
    )
    assert (reused.input_document_type, reused.input_profile_ref) == (
        "singlexml", _LITERAL_PROFILE_ID,
    )
    assert reused.connection_ref is None and reused.action_type == "Listen"

    # ...and the submitted bytes for a raw create.
    submitted_xml = _built(_wss_config(input_type="multixml"))
    raw = _spec("wss_op", "connector-action", {**config, "xml": submitted_xml})
    submitted = _symbol(_table([conn, rest_op, raw]), "wss_op")
    assert (submitted.input_document_type, submitted.input_profile_ref) == (
        "multixml", None,
    )

    # A WSS config with no input type gets the builder's default, as it stores.
    defaulted = _symbol(_table([_spec("wss_op", "connector-action", _wss_config())]),
                        "wss_op")
    assert defaulted.input_document_type == WssListenerOperationBuilder.DEFAULT_INPUT_TYPE
    assert defaulted.input_profile_ref is None

    # A non-WSS component gets neither field — with or without a snapshot.
    for table in (_table(components), _table(components, snapshot=None)):
        for key in ("op", "conn"):
            symbol = _symbol(table, key)
            assert (symbol.input_document_type, symbol.input_profile_ref) == (
                None, None,
            ), key
        assert _symbol(table, "op").connection_ref == "$ref:conn"
        assert not is_listener_operation_symbol(_symbol(table, "op"))

    # A connection arriving on a WSS operation's symbol is CARRIED, not dropped,
    # so the listener entry refuses the invented binding instead of ignoring it.
    bound = _spec("wss_op", "connector-action", {**config, "connection_ref_key": "conn"})
    table = _table([conn, rest_op, bound], snapshot=None)
    assert _symbol(table, "wss_op").connection_ref == "$ref:conn"
    assert _compile_codes(_listener_doc(), table) == [
        (PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID, "/body/steps/0/operation_ref"),
    ]


# ---------------------------------------------------------------------------
# the public plan / compile route
# ---------------------------------------------------------------------------


def _request(listener_operation="$ref:wss_op", wss_overrides=None, inbound_validation=None):
    """A typed request whose canonical listener root lives in ``integration_spec.processes``."""
    wss_op = {
        "key": "wss_op",
        "type": "connector-action",
        "name": "158 listener op",
        "action": "create",
        "config": _wss_config(**{"object_name": "orderIntake158",
                                 "input_type": "singlejson", **(wss_overrides or {})}),
    }
    assert "action_type" not in wss_op["config"], "the premise is a NATIVE config"
    post_op = copy.deepcopy(APPLIABLE_OP)
    post_op["config"]["method"] = "POST"
    unit = ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key="proc", name="158 Listener", action="create",
            depends_on=("conn", "op", "wss_op"),
        ),
        process_ir=parse_process_ir_v1(
            _listener_doc(listener_operation, inbound_validation)
        ),
    )
    spec = IntegrationSpecV1(
        name="158 Integration",
        components=[APPLIABLE_CONN, post_op, wss_op],
        processes=[unit],
    )
    return AuthoringRequestV1(intent=IntegrationSpecAuthoringIntentV1(integration_spec=spec))


def _served(action, request):
    with patch(_PAGINATE, return_value=[]):
        return integration_builder.build_integration_action(
            MagicMock(), _PROFILE, action,
            config={"authoring_request": request.model_dump(mode="json")},
        )


def _served_errors(result):
    return list((result.get("authoring_result") or {}).get("errors") or ())


def _all_cause_codes(result):
    """Every code a served envelope carries, wherever the surface puts it."""
    codes = {result.get("error_code")}
    payload = result.get("authoring_result") or {}
    diagnostics = list(result.get("authoring_diagnostics") or ())
    for bucket in ("errors", "warnings", "diagnostics"):
        diagnostics.extend(payload.get(bucket) or ())
    for diagnostic in diagnostics:
        codes.add(diagnostic.get("code"))
        codes.update(diagnostic.get("cause_codes") or ())
    return {code for code in codes if code}


def test_listener_entry_resolves_a_native_wss_operation_end_to_end(monkeypatch):
    """A native WSS operation is a listener's operation, through the public route."""
    for action in ("plan", "compile"):
        result = _served(action, _request())
        assert result["_success"] is True, (action, result.get("error"))
        assert _served_errors(result) == [], (action, _served_errors(result))
        assert result["authoring_result"]["validation_report"]["is_valid"] is True
        assert PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID not in _all_cause_codes(result)
    # Compile serves the CFG it built for the root: listener -> target -> stop.
    assert [
        (c["component_key"], c["node_count"], c["edge_count"], c["terminal_kinds"])
        for c in result["authoring_result"]["process_cfg"]
    ] == [("proc", 3, 2, ["stop"])]

    # The listener's operation swapped for the REST operation is refused.
    swapped = _request(listener_operation="$ref:op")
    plan = _served("plan", swapped)
    assert plan["authoring_result"]["validation_report"]["is_valid"] is False
    assert PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID in _all_cause_codes(plan)
    assert [
        (d["path"], d["subject_id"]) for d in _served_errors(plan)
        if PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID in (d.get("cause_codes") or ())
    ] == [("/body/steps/0/operation_ref", "proc")]
    compiled = _served("compile", swapped)
    assert compiled["_success"] is False
    assert PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID in _all_cause_codes(compiled)

    # NON-VACUITY: disable the shared derivation and the SAME native spec is
    # refused — the clean result above is the derivation's, not an accident.
    monkeypatch.setattr(connector_builder, "wss_listen_action_from_config",
                        lambda config: None)
    underived = _served("plan", _request())
    assert underived["authoring_result"]["validation_report"]["is_valid"] is False
    assert PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID in _all_cause_codes(underived)


def test_inbound_validation_reads_the_resolved_operation():
    """``profile_bound`` is decided by the operation's own facts, read from config."""
    conn, rest_op = _rest_components()
    profile = _spec("order_request_profile", "profile.json", {})
    profile_bound = {"mode": "profile_bound"}

    def _codes(inbound, **wss_overrides):
        wss_op = _spec("wss_op", "connector-action", _wss_config(**wss_overrides))
        table = _table([conn, rest_op, profile, wss_op])
        return _compile_codes(_listener_doc(inbound_validation=inbound), table)

    unsatisfied = [(
        PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED,
        "/body/steps/0/inbound_validation",
    )]

    passing = [
        dict(input_type="singlejson", request_profile="$ref:order_request_profile"),
        dict(input_type="singlexml", request_profile="$ref:order_request_profile"),
        dict(input_type="singlejson", request_profile=_LITERAL_PROFILE_ID),
        dict(input_type="singlexml", request_profile=_LITERAL_PROFILE_ID),
        # The builder's default input type is singlejson, and it counts as such.
        dict(request_profile="$ref:order_request_profile"),
        # Every other profile-bindable type the builder admits.
        dict(input_type="multijson", request_profile="$ref:order_request_profile"),
        dict(input_type="multixml", request_profile=_LITERAL_PROFILE_ID),
    ]
    for overrides in passing:
        assert _codes(profile_bound, **overrides) is None, overrides

    refused = [
        dict(input_type="none"),
        dict(input_type="singledata"),
        # A profile beside a non-document type still does not satisfy it.
        dict(input_type="none", request_profile="$ref:order_request_profile"),
        dict(input_type="singledata", request_profile=_LITERAL_PROFILE_ID),
        # A JSON/XML type with nothing bound.
        dict(input_type="singlejson"),
        dict(input_type="singlexml", request_profile="   "),
        # A `$ref` naming something that is not a profile binds nothing.
        dict(input_type="singlejson", request_profile="$ref:conn"),
        dict(input_type="singlejson", request_profile="$ref:no_such_profile"),
    ]
    for overrides in refused:
        assert _codes(profile_bound, **overrides) == unsatisfied, overrides

    # An absent inbound_validation adds no requirement — even on an operation
    # that could never satisfy profile_bound.
    for overrides in (dict(input_type="none"), dict(input_type="singledata"),
                      dict(input_type="singlejson")):
        assert _codes(None, **overrides) is None, overrides

    # The facts are the RESOLVED operation's: a reused operation whose stored
    # bytes differ from its config is judged by the bytes, in both directions.
    def _reused_codes(config_overrides, stored_overrides):
        wss_op = _spec("wss_op", "connector-action", _wss_config(**config_overrides))
        stored = _platform_readback(_built(_wss_config(**stored_overrides)))
        table = _table([conn, rest_op, profile, wss_op],
                       live_component_xml={"wss_op": stored}, reused_keys={"wss_op"})
        return _compile_codes(_listener_doc(inbound_validation=profile_bound), table)

    assert _reused_codes(
        dict(input_type="none"),
        dict(input_type="singlejson", request_profile="$ref:order_request_profile"),
    ) is None
    assert _reused_codes(
        dict(input_type="singlejson", request_profile="$ref:order_request_profile"),
        dict(input_type="none"),
    ) == unsatisfied

    # ...and the same verdicts reach the public plan route.
    bound = _served("plan", _request(
        wss_overrides={"request_profile": _LITERAL_PROFILE_ID},
        inbound_validation=profile_bound,
    ))
    assert bound["authoring_result"]["validation_report"]["is_valid"] is True, (
        _served_errors(bound)
    )
    unbound = _served("plan", _request(
        wss_overrides={"input_type": "none"}, inbound_validation=profile_bound,
    ))
    assert unbound["authoring_result"]["validation_report"]["is_valid"] is False
    assert [
        d["path"] for d in _served_errors(unbound)
        if PROCESS_IR_SEMANTIC_LISTENER_INBOUND_CONTRACT_UNSATISFIED
        in (d.get("cause_codes") or ())
    ] == ["/body/steps/0/inbound_validation"]


# ---------------------------------------------------------------------------
# QA-158-r1-01 / r1-03: the served text a refused listener operation meets
# ---------------------------------------------------------------------------


def _cited_entry_text(authoring_entry_id):
    """An entry the refusal cites, resolved through the PUBLIC selector."""
    from boomi_mcp.authoring.process_ir_projection import query_process_ir_authoring_contract

    page = query_process_ir_authoring_contract(authoring_entry_id=authoring_entry_id)
    [entry] = page.entries
    return " ".join((entry.summary,) + tuple(entry.ordering_facts))


def test_a_wss_listen_operation_called_as_a_connector_call_is_pointed_at_the_listener():
    """QA-158-r1-01: the connector-call refusal sends a WSS Listen caller to the listener.

    A WSS Listen operation has no row in the connector-call allowlist, so calling
    it is refused. The refusal cites the diagnostic's contract entry, and that
    entry — resolved through the public selector a caller would use — names the
    listener node's selector, which in turn serves ``node.listener``. The earlier
    claim rested on a module table with no consumer; this is the text a caller
    actually receives.
    """
    from boomi_mcp.authoring.process_ir_projection import query_process_ir_authoring_contract
    from boomi_mcp.errors import PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED

    wss_op = {
        "key": "wss_op",
        "type": "connector-action",
        "name": "158 listener op",
        "action": "create",
        "config": _wss_config(object_name="orderIntake158"),
    }
    unit = ProcessAuthoringUnitV1(
        envelope=ProcessComponentEnvelopeV1(
            component_key="proc", name="158 Call", action="create", depends_on=("wss_op",),
        ),
        process_ir=parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
            {"kind": "connector_call", "operation_ref": "$ref:wss_op"},
            {"kind": "stop"},
        ]}}),
    )
    request = AuthoringRequestV1(intent=IntegrationSpecAuthoringIntentV1(
        integration_spec=IntegrationSpecV1(name="158 Call", components=[wss_op], processes=[unit])
    ))

    compiled = _served("compile", request)
    assert compiled["_success"] is False
    refusals = [
        d for d in compiled.get("authoring_diagnostics") or ()
        if PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED in (d.get("cause_codes") or ())
    ]
    assert [d["path"] for d in refusals] == ["/body/steps/0/operation_ref"], refusals
    cited = refusals[0]["authoring_contract_entry_ids"]
    diagnostic_id = "diagnostic.process_ir_capability_connector_action_unsupported"
    assert diagnostic_id in cited, cited

    selector = "node_kind='listener'"
    assert selector in _cited_entry_text(diagnostic_id)
    # ...and that selector serves the listener node.
    listener_page = query_process_ir_authoring_contract(node_kind="listener")
    assert "node.listener" in {entry.contract_entry_id for entry in listener_page.entries}


def test_a_listener_operation_is_recognised_by_its_declaration_and_the_refusal_says_so():
    """QA-158-r1-03: the rule is the operation's DECLARED configuration, and the text says so.

    A reused operation whose account bytes ARE a WSS Listen operation, declared
    without ``operation_mode``, is refused as a listener's operation; declaring
    the builder's one supported mode makes the same root compile. The served
    message and remediation state exactly that rule — the declaration, the key
    and value that flip the outcome, and that a reused operation carries it too —
    instead of claiming the operation is not a Listen operation.
    """
    from boomi_mcp.compiler.process_ir import diagnostics as compiler_diagnostics

    conn, rest_op = _rest_components()
    stored = _platform_readback(_built(_wss_config()))
    undeclared = {
        "reference_only": True,
        "connector_type": "wss",
        "component_id": "0f5a9d7e-1580-4b1c-9e58-158158158158",
    }

    def _codes(config):
        wss_op = _spec("wss_op", "connector-action", config)
        table = _table([conn, rest_op, wss_op],
                       live_component_xml={"wss_op": stored}, reused_keys={"wss_op"})
        return _compile_codes(_listener_doc(), table)

    # NON-VACUITY: the account's own bytes read as a Web Services Server Listen
    # operation, so the refusal below is about the declaration, not the bytes.
    identity = live_identity_from_component_xml("wss_op", stored)
    assert (identity.family, identity.action) == ("wss", "LISTEN")

    refused = [(PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID, "/body/steps/0/operation_ref")]
    assert _codes(undeclared) == refused
    (mode,) = WssListenerOperationBuilder.SUPPORTED_OPERATION_MODES
    assert _codes({**undeclared, "operation_mode": mode}) is None

    remediation = compiler_diagnostics._REMEDIATION[PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID]
    message = compiler_diagnostics._MESSAGES[PROCESS_IR_REFERENCE_LISTENER_OPERATION_INVALID]
    for text in (remediation, message):
        assert "declared" in text, text
        assert "is not a Web Services Server Listen operation" not in text, text
    assert "operation_mode" in remediation and "'{0}'".format(mode) in remediation
    assert "reference_only" in remediation
    # The served entry carries the same wording a caller receives.
    served = _cited_entry_text("diagnostic.process_ir_reference_listener_operation_invalid")
    assert "operation_mode" in served and "reference_only" in served
