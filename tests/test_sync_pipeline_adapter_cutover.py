"""Cut-over tests for the sync_pipeline legacy adapter (issue #139 M12.4, slice C).

``SyncPipelineBuilder.build`` now lowers its pipeline and emits the resulting
linear core through the ONE canonical chain (ProcessIRV1 -> compile_process_ir_v1
-> emit_process) instead of ProcessFlowBuilder's own renderer -- for its six
non-listener stage chains. The four WSS listener chains stay on the legacy
renderer behind an explicit routing gate (#140 owns the fused ``start_listen``
entry).

The primary oracle here is DIFFERENTIAL. Because the interception happens inside
``SyncPipelineBuilder.build`` *before* it would delegate, ``ProcessFlowBuilder``
on the same lowered config remains the untouched legacy renderer -- so every
migrated chain can be asserted against genuinely independent code, forever. The
committed byte goldens in test_sync_pipeline_builder.py are the complement: they
catch a uniform drift that would move both sides of the differential together.
"""

from __future__ import annotations

import copy

import pytest

from boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)
from boomi_mcp.categories.components.builders.process_flow_builder import (
    ProcessFlowBuilder,
    SyncPipelineBuilder,
    _sync_pipeline_is_canonical,
)
from boomi_mcp.compiler.process_ir.legacy_adapters import sync_pipeline as adapter_module
from boomi_mcp.compiler.process_ir.legacy_adapters.contracts import (
    LEGACY_ADAPTER_ALIAS_PREFIX,
    LegacyAdapterError,
)
from boomi_mcp.compiler.process_ir.legacy_adapters.sync_pipeline import (
    adapt_sync_pipeline,
)

# ---------------------------------------------------------------------------
# Config fixtures -- the 10 accepted stage chains
# ---------------------------------------------------------------------------


def _stage(key, kind, defaults, over):
    """A stage whose config defaults every caller may override key-by-key."""
    config = dict(defaults)
    config.update(over)
    return {"key": key, "kind": kind, "config": config}


_SRC = {"connection_id": "SRC-CONN", "operation_id": "SRC-OP"}
_TGT = {"connection_id": "TGT-CONN", "operation_id": "TGT-OP"}


def _db_read(key="s", **cfg):
    return _stage(key, "read", {"primitive": "db_read", **_SRC}, cfg)


def _rest_fetch(key="s", **cfg):
    return _stage(key, "fetch", {"primitive": "rest_fetch", **_SRC}, cfg)


def _soap_fetch(key="s", **cfg):
    return _stage(key, "fetch", {"primitive": "soap_fetch", **_SRC}, cfg)


def _map(key="m", **cfg):
    return _stage(key, "map", {"primitive": "map", "map_ref": "MAP-1"}, cfg)


def _rest_send(key="t", **cfg):
    return _stage(key, "send", {"primitive": "rest_send", "action_type": "POST", **_TGT}, cfg)


def _soap_send(key="t", **cfg):
    return _stage(key, "send", {"primitive": "soap_send", **_TGT}, cfg)


def _db_write(key="t", **cfg):
    return _stage(key, "write", {"primitive": "db_write", **_TGT}, cfg)


def _listen(key="s", **cfg):
    return _stage(key, "listener", {"primitive": "wss_listen", "operation_id": "WSSOP-1"}, cfg)


def _pipeline(stages, **top):
    keys = [s["key"] for s in stages]
    return {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": stages,
            "dependencies": [
                {"from_stage": a, "to_stage": b} for a, b in zip(keys, keys[1:])
            ],
        },
        **top,
    }


# The six chains #139C migrates to the canonical chain.
MIGRATED_CHAINS = {
    "read_send": [_db_read(), _rest_send()],
    "read_map_send": [_db_read(), _map(), _rest_send()],
    "fetch_send": [_rest_fetch(), _rest_send()],
    "fetch_map_send": [_rest_fetch(), _map(), _rest_send()],
    "fetch_write": [_rest_fetch(), _db_write()],
    "fetch_map_write": [_rest_fetch(), _map(), _db_write()],
}
# ...plus the SOAP family, orthogonal to stage kind (declared by the primitive),
# including the non-uppercase `execute` verb the pre-#139C canonicalizer corrupted.
MIGRATED_CHAINS.update(
    {
        "soap_fetch_soap_send": [_soap_fetch(), _soap_send()],
        "soap_lowercase_execute": [
            _soap_fetch(action_type="execute"),
            _soap_send(action_type="execute"),
        ],
    }
)

# The four chains that stay on the legacy renderer (#140).
LISTENER_CHAINS = {
    "listener_send": [_listen(), _rest_send()],
    "listener_map_send": [_listen(), _map(), _rest_send()],
    "listener_write": [_listen(), _db_write()],
    "listener_map_write": [_listen(), _map(), _db_write()],
}


# ---------------------------------------------------------------------------
# 1. The differential byte oracle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", sorted(MIGRATED_CHAINS))
def test_cutover_is_byte_identical_to_the_legacy_renderer(chain):
    """The canonical chain must emit exactly what the legacy renderer emits.

    This identity held before the cut-over too (build() simply delegated), so it
    is a true before/after invariant rather than a tautology.
    """
    cfg = _pipeline(copy.deepcopy(MIGRATED_CHAINS[chain]))
    lowered = SyncPipelineBuilder.lower_config(cfg)
    emitted = SyncPipelineBuilder.build(cfg, name="P", folder_name="F")
    assert emitted == ProcessFlowBuilder.build(lowered, name="P", folder_name="F")
    assert LEGACY_ADAPTER_ALIAS_PREFIX not in emitted


@pytest.mark.parametrize("chain", sorted(LISTENER_CHAINS))
def test_listener_chains_still_match_the_legacy_renderer(chain):
    cfg = _pipeline(copy.deepcopy(LISTENER_CHAINS[chain]))
    lowered = SyncPipelineBuilder.lower_config(cfg)
    assert SyncPipelineBuilder.build(cfg, name="P", folder_name="F") == (
        ProcessFlowBuilder.build(lowered, name="P", folder_name="F")
    )


def test_db_write_target_keeps_its_mixed_case_send_verb():
    """The whole reason #139C had to correct the compiler's canonicalizer.

    The legacy builder emits a DB write target's verb VERBATIM as the mixed-case
    ``Send`` (mirroring the DB source's ``Get``); before #139C the compiler
    upper-cased every target verb regardless of family, so the canonical chain
    emitted ``SEND``.

    The justification is byte-parity with the legacy renderer, and nothing more.
    Live QA established that a ``SEND`` build does still package, deploy and
    execute successfully against a 26.07 atom -- so do NOT restate this as "SEND
    is undeployable". Divergence from the renderer we are replacing is the whole
    contract of this migration; it needs no further consequence to matter.
    """
    xml = SyncPipelineBuilder.build(
        _pipeline([_rest_fetch(), _map(), _db_write()]), name="P"
    )
    assert 'connectorType="database"' in xml
    assert 'actionType="Send"' in xml
    assert 'actionType="SEND"' not in xml


def test_soap_target_keeps_a_non_uppercase_verb_verbatim():
    xml = SyncPipelineBuilder.build(
        _pipeline([_soap_fetch(action_type="execute"), _soap_send(action_type="execute")]),
        name="P",
    )
    assert xml.count('connectorType="wssoapclientsdk"') == 2
    assert xml.count('actionType="execute"') == 2
    assert 'actionType="EXECUTE"' not in xml


# ---------------------------------------------------------------------------
# 2. Occurrence-scoped alias reuse (the #139B control-substitution oracle)
# ---------------------------------------------------------------------------


def _assert_reuse_byte_faithful(shared_stages, control_stages, subs):
    """A config that REUSES one id must emit exactly what the distinct-id control
    emits with each distinct id replaced by the shared id -- so reuse stays
    byte-faithful -- and no alias may leak into the XML."""
    shared_xml = SyncPipelineBuilder.build(_pipeline(shared_stages), name="P")
    expected = SyncPipelineBuilder.build(_pipeline(control_stages), name="P")
    for distinct, shared in subs.items():
        expected = expected.replace(distinct, shared)
    assert shared_xml == expected
    assert LEGACY_ADAPTER_ALIAS_PREFIX not in shared_xml


def test_one_id_reused_across_all_four_connector_slots_is_byte_faithful():
    shared = [
        _db_read(connection_id="ZZZ9", operation_id="ZZZ9"),
        _rest_send(connection_id="ZZZ9", operation_id="ZZZ9"),
    ]
    control = [
        _db_read(connection_id="AAA1", operation_id="BBB2"),
        _rest_send(connection_id="CCC3", operation_id="DDD4"),
    ]
    _assert_reuse_byte_faithful(
        shared, control, {"AAA1": "ZZZ9", "BBB2": "ZZZ9", "CCC3": "ZZZ9", "DDD4": "ZZZ9"}
    )


def test_map_ref_reusing_a_connection_id_is_byte_faithful():
    """The cross-TYPE reuse case: one id serving as both a connection and a map.

    Without occurrence-scoped aliases these collapse into a single symbol whose
    component_type cannot be both, which is the bug class #139B closed.
    """
    shared = [_db_read(connection_id="ZZZ9"), _map(map_ref="ZZZ9"), _rest_send()]
    control = [_db_read(connection_id="AAA1"), _map(map_ref="BBB2"), _rest_send()]
    _assert_reuse_byte_faithful(shared, control, {"AAA1": "ZZZ9", "BBB2": "ZZZ9"})


# ---------------------------------------------------------------------------
# 3. Routing is EXPLICIT, not incidental
# ---------------------------------------------------------------------------


@pytest.fixture
def adapter_spy(monkeypatch):
    calls = []
    original = adapter_module.adapt_sync_pipeline

    def spy(config):
        calls.append(config)
        return original(config)

    monkeypatch.setattr(adapter_module, "adapt_sync_pipeline", spy)
    return calls


def test_non_listener_chain_actually_goes_through_the_adapter(adapter_spy):
    """Byte equality alone cannot tell a cut-over from a no-op -- pin the routing."""
    SyncPipelineBuilder.build(_pipeline([_rest_fetch(), _map(), _db_write()]), name="P")
    assert len(adapter_spy) == 1


def test_listener_chain_does_not_reach_the_adapter(adapter_spy):
    SyncPipelineBuilder.build(_pipeline([_listen(), _map(), _rest_send()]), name="P")
    assert adapter_spy == []


def test_archetype_style_direct_process_flow_build_does_not_reach_the_adapter(adapter_spy):
    """The database_to_api_sync archetype calls ``lower_config`` and then builds
    through ``ProcessFlowBuilder`` (never ``SyncPipelineBuilder.build``), adding
    ``reliability`` / ``dynamic_path`` that this dialect cannot carry. The
    interception point must leave that caller on the legacy renderer."""
    lowered = SyncPipelineBuilder.lower_config(_pipeline([_rest_fetch(), _map(), _db_write()]))
    ProcessFlowBuilder.build(lowered, name="P", folder_name="F")
    assert adapter_spy == []


@pytest.mark.parametrize(
    "connector_type,canonical",
    [
        ("wss", False), ("WSS", False), ("  wss  ", False),
        ("web_services", False), ("web_services_server", False),
        ("database", True), ("rest", True), ("rest_client", True),
        ("soap_client", True), (None, True), ("", True),
    ],
)
def test_routing_gate_matches_the_legacy_listener_predicate(connector_type, canonical):
    assert _sync_pipeline_is_canonical({"source": {"connector_type": connector_type}}) is canonical


# ---------------------------------------------------------------------------
# 4. Gate 2 -- the adapter refuses a listener even if routed one
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "connector_type",
    [
        "wss", "WSS", "  wss  ", "web_services", "web_services_server",
        # The routing gate mirrors the LEGACY selector, which does not resolve
        # these two; the refusal gate mirrors the COMPILER's set, which does. No
        # reachable config can tell them apart (lower_config admits only the wss
        # aliases), but refusing here turns a deep PROCESS_IR_CAPABILITY_UNSUPPORTED
        # into a precise adapter pointer -- and #140 promotes this adapter to a
        # dialect whose input is not pre-filtered.
        "wssserver", "listener",
    ],
)
def test_adapter_refuses_a_listener_source(connector_type):
    """The independent backstop: a caller must not be able to route a listener
    past the builder's gate and get a silently mis-shaped start_noaction pair."""
    core = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": connector_type, "action_type": "Listen",
                   "operation_id": "WSSOP-1"},
        "transform": {"mode": "passthrough"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "TC", "operation_id": "TO"},
    }
    with pytest.raises(LegacyAdapterError) as exc:
        adapt_sync_pipeline(core)
    diag = exc.value.diagnostics[0]
    assert diag.code == "LEGACY_ADAPTER_UNSUPPORTED_KIND"
    assert diag.legacy_source_path == "/source/connector_type"


# ---------------------------------------------------------------------------
# 5. compatibility_noop_paths is always empty for this fail-closed dialect
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", sorted(MIGRATED_CHAINS))
def test_no_compatibility_noop_paths(chain):
    """The dialect's config gate is a strict allow-list at every level, so nothing
    is accepted-and-ignored. A failure here means someone loosened that gate."""
    lowered = SyncPipelineBuilder.lower_config(_pipeline(copy.deepcopy(MIGRATED_CHAINS[chain])))
    assert adapt_sync_pipeline(lowered).compatibility_noop_paths == ()


# ---------------------------------------------------------------------------
# 6. The symbol-requirement contract
# ---------------------------------------------------------------------------


def test_symbol_requirement_contract_for_read_map_send():
    lowered = SyncPipelineBuilder.lower_config(
        _pipeline([_db_read(label="Read"), _map(label="Map"), _rest_send(label="Send")])
    )
    reqs = adapt_sync_pipeline(lowered).symbol_requirements
    p = LEGACY_ADAPTER_ALIAS_PREFIX
    # Emission order is FLOW order, and every pointer is unique so no two
    # requirements can share an ir_ref.
    assert [
        (r.role, r.ir_ref, r.legacy_selector, r.source_pointer, r.expected_component_type)
        for r in reqs
    ] == [
        ("connectoraction_source.connection", p + "/source/connection_id", "SRC-CONN",
         "/source/connection_id", "connector-settings"),
        ("connectoraction_source.operation", p + "/source/operation_id", "SRC-OP",
         "/source/operation_id", "connector-action"),
        ("map", p + "/transform/map_ref", "MAP-1", "/transform/map_ref", "transform.map"),
        ("connectoraction_target.connection", p + "/target/connection_id", "TGT-CONN",
         "/target/connection_id", "connector-settings"),
        ("connectoraction_target.operation", p + "/target/operation_id", "TGT-OP",
         "/target/operation_id", "connector-action"),
    ]
    # Connector metadata rides ONLY on the two operation requirements.
    assert [(r.connector_type, r.action_type) for r in reqs] == [
        (None, None), ("database", "Get"), (None, None), (None, None), ("rest", "POST"),
    ]
    assert len({r.ir_ref for r in reqs}) == len(reqs)


def test_adapter_carries_no_envelope_data_into_the_ir():
    """ADR-001 §6: description / process_extensions are envelope concerns the
    component assembler owns; they must not appear anywhere in the IR."""
    lowered = SyncPipelineBuilder.lower_config(
        _pipeline(
            [_db_read(), _rest_send()],
            description="SENTINEL-DESCRIPTION",
            process_extensions={
                "connections": [
                    {"connection_id": "SRC-CONN", "connector_type": "database",
                     "fields": [{"id": "url", "label": "URL", "xpath": "//url"}]}
                ]
            },
        )
    )
    blob = adapt_sync_pipeline(lowered).process_ir.model_dump_json()
    assert "SENTINEL-DESCRIPTION" not in blob
    assert "process_extensions" not in blob
    assert "//url" not in blob


# ---------------------------------------------------------------------------
# 6b. source_pointer is EXACT for whichever document the caller passed
# ---------------------------------------------------------------------------


def _rfc6901(doc, pointer):
    """Minimal RFC 6901 resolver -- deliberately not the production one, so this
    tests the pointers rather than agreeing with the code that built them."""
    cur = doc
    for token in pointer.lstrip("/").split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(cur, list):
            try:
                cur = cur[int(token)]
            except (ValueError, IndexError):
                return _MISSING
        elif isinstance(cur, dict):
            if token not in cur:
                return _MISSING
            cur = cur[token]
        else:
            return _MISSING
    return cur


_MISSING = object()


def _out_of_flow_order_config():
    """Stages listed target, source, map -- flow order is source -> map -> target.

    `stages` is an unordered set whose sequence comes from `dependencies`, so raw
    LIST INDEX and FLOW POSITION disagree here. A remapping that used flow position
    would produce pointers that still RESOLVE but name the WRONG stage, which is
    worse than not resolving; this ordering is what makes that failure visible.
    """
    return {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [_rest_send("t"), _db_read("s"), _map("m")],
            "dependencies": [
                {"from_stage": "s", "to_stage": "m"},
                {"from_stage": "m", "to_stage": "t"},
            ],
        },
    }


def test_registry_entry_pointers_resolve_against_the_raw_config():
    """`source_pointer` is contractually the EXACT pointer to the originating
    field, so it must resolve -- in the document the CALLER handed over."""
    from boomi_mcp.compiler.process_ir.legacy_adapters import (
        SYNC_PIPELINE_DIALECT,
        adapter_for,
    )

    raw = _out_of_flow_order_config()
    reqs = adapter_for(SYNC_PIPELINE_DIALECT)(raw).symbol_requirements
    assert reqs, "expected requirements to pin"
    for req in reqs:
        resolved = _rfc6901(raw, req.source_pointer)
        assert resolved is not _MISSING, f"{req.source_pointer} does not resolve"
        # Not merely resolvable -- it must name the field holding THIS value, which
        # is what catches a right-shape/wrong-stage remap.
        assert resolved == req.legacy_selector, req.source_pointer
    # And each alias embeds the same raw pointer, so the two cannot drift.
    for req in reqs:
        assert req.ir_ref == LEGACY_ADAPTER_ALIAS_PREFIX + req.source_pointer
    assert all(r.source_pointer.startswith("/pipeline/stages/") for r in reqs)


def test_builder_entry_pointers_stay_relative_to_the_lowered_core():
    """The other entry point passes the lowered core, so its pointers are rooted
    there. Both are exact -- for different documents."""
    core = SyncPipelineBuilder.lower_config(_out_of_flow_order_config())
    reqs = adapt_sync_pipeline(core).symbol_requirements
    for req in reqs:
        assert _rfc6901(core, req.source_pointer) == req.legacy_selector
    assert {r.source_pointer for r in reqs} == {
        "/source/connection_id", "/source/operation_id", "/transform/map_ref",
        "/target/connection_id", "/target/operation_id",
    }


@pytest.mark.parametrize(
    "map_config,expected_key",
    [
        ({"map_ref": "MAP-1"}, "map_ref"),
        # `lower_config` canonicalizes BOTH spellings to `transform.map_ref`, so the
        # lowered core cannot tell them apart and the raw key must be carried over.
        ({"map_id": "MAP-1"}, "map_id"),
        # The nastiest of the three: an empty `map_ref` alongside a real `map_id`.
        # Naming `map_ref` here does not merely fail to resolve -- it RESOLVES, to
        # the empty string, silently reporting the wrong field.
        ({"map_ref": "", "map_id": "MAP-1"}, "map_id"),
    ],
)
def test_map_pointer_names_the_spelling_the_author_actually_used(map_config, expected_key):
    from boomi_mcp.compiler.process_ir.legacy_adapters import (
        SYNC_PIPELINE_DIALECT,
        adapter_for,
    )

    raw = _pipeline(
        [_db_read("s"), _stage("m", "map", {"primitive": "map"}, map_config), _rest_send("t")]
    )
    reqs = adapter_for(SYNC_PIPELINE_DIALECT)(raw).symbol_requirements
    map_req = next(r for r in reqs if r.role == "map")
    assert map_req.source_pointer == f"/pipeline/stages/1/config/{expected_key}"
    # Resolvable is not enough -- it must hold THIS requirement's value.
    assert _rfc6901(raw, map_req.source_pointer) == map_req.legacy_selector
    # And the emitted XML is identical whichever spelling was authored.
    assert SyncPipelineBuilder.build(raw, name="P", folder_name="F") == (
        ProcessFlowBuilder.build(
            SyncPipelineBuilder.lower_config(raw), name="P", folder_name="F"
        )
    )


def test_registry_listener_diagnostic_points_at_the_primitive_not_connector_type():
    """A raw listener stage is identified by its PRIMITIVE. `connector_type` is
    accepted there but wholly inert -- every value emits identical XML and none of
    them selects the listener path -- so a diagnostic aimed at it would misdirect."""
    from boomi_mcp.compiler.process_ir.legacy_adapters import (
        SYNC_PIPELINE_DIALECT,
        adapter_for,
    )

    raw = _pipeline([_listen(), _rest_send()])
    with pytest.raises(LegacyAdapterError) as exc:
        adapter_for(SYNC_PIPELINE_DIALECT)(raw)
    pointer = exc.value.diagnostics[0].legacy_source_path
    assert _rfc6901(raw, pointer) == "wss_listen"


# ---------------------------------------------------------------------------
# 7. Fail-closed guards
# ---------------------------------------------------------------------------


def _core(**over):
    core = {
        "process_kind": "database_to_api_sync",
        "source": {"connector_type": "database", "action_type": "Get",
                   "connection_id": "SC", "operation_id": "SO"},
        "transform": {"mode": "map_ref", "map_ref": "M1"},
        "target": {"connector_type": "rest", "action_type": "POST",
                   "connection_id": "TC", "operation_id": "TO"},
    }
    core.update(over)
    return core


@pytest.mark.parametrize(
    "mutate,code,pointer",
    [
        (lambda c: c.__setitem__("flow_sequence", []),
         "LEGACY_ADAPTER_UNSUPPORTED_KIND", "/flow_sequence"),
        (lambda c: c.__setitem__("reliability", {"try_catch": True}),
         "LEGACY_ADAPTER_UNSUPPORTED_KIND", "/reliability"),
        (lambda c: c["target"].__setitem__("dynamic_path", {"value_source": "static"}),
         "LEGACY_ADAPTER_UNSUPPORTED_KIND", "/target/dynamic_path"),
        (lambda c: c["transform"].__setitem__("mode", "dataprocess"),
         "LEGACY_ADAPTER_UNSUPPORTED_KIND", "/transform/mode"),
        (lambda c: c["source"].__setitem__("connection_id", ""),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/source/connection_id"),
        (lambda c: c["source"].__setitem__("connection_id", None),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/source/connection_id"),
        (lambda c: c["source"].__setitem__("operation_id", "   "),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/source/operation_id"),
        (lambda c: c["target"].__setitem__("connection_id", ""),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/target/connection_id"),
        (lambda c: c["target"].__setitem__("operation_id", None),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/target/operation_id"),
        (lambda c: c["transform"].__setitem__("map_ref", ""),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/transform/map_ref"),
        (lambda c: c.__setitem__("source", "not-a-dict"),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/source"),
        (lambda c: c.__setitem__("target", 7),
         "LEGACY_ADAPTER_SEMANTIC_LOSS", "/target"),
    ],
)
def test_adapter_fails_closed(mutate, code, pointer):
    """Never silently drop a construct that would change the emitted XML.

    Each of these is reachable only through a direct ``build()`` bypass carrying a
    hand-crafted core, but every one of them changes the shape sequence -- so the
    adapter must raise rather than emit a quietly different flow.
    """
    core = _core()
    mutate(core)
    with pytest.raises(LegacyAdapterError) as exc:
        adapt_sync_pipeline(core)
    diag = exc.value.diagnostics[0]
    assert (diag.code, diag.legacy_source_path) == (code, pointer)


def test_missing_connection_id_surfaces_as_the_builder_error_not_a_raw_validation_error():
    """A raw pydantic ValidationError is NOT in the cut-over's caught tuple.

    Without the adapter's own empty-id guard, an unresolved binding id would fail
    ``LegacySymbolRequirementV1``'s ``min_length`` and escape ``build()`` as an
    unhandled pydantic error rather than the builder's documented failure.
    """
    cfg = _pipeline(
        [
            {"key": "s", "kind": "read", "config": {"primitive": "db_read", "operation_id": "SO"}},
            _rest_send(),
        ]
    )
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(cfg, name="P")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"
    assert exc.value.field == "config"
    # The internal adapter diagnostic is chained, never surfaced.
    assert isinstance(exc.value.__cause__, LegacyAdapterError)
    assert not str(exc.value).startswith("SYNC_PIPELINE")


def test_blank_name_still_raises_the_name_error_first():
    """The cut-over intercepts before delegating, so ProcessFlowBuilder's own name
    guard no longer runs first -- it is reproduced, and its precedence pinned."""
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(_pipeline([_db_read(), _rest_send()]), name="   ")
    assert exc.value.error_code == "PROCESS_XML_VALIDATION_FAILED"
    assert exc.value.field == "name"


def test_lowering_errors_still_outrank_the_reproduced_name_guard():
    """The other half of the precedence contract, and the easier half to break.

    The reproduced name guard sits AFTER ``lower_config``, exactly where
    ProcessFlowBuilder's sits relative to it. Hoisting it above ``lower_config`` --
    a natural-looking simplification -- would silently promote the name error over
    every ``SYNC_PIPELINE_*`` code whenever a caller gets both wrong at once. The
    test above uses a VALID pipeline and so cannot catch that.
    """
    both_wrong = _pipeline([_db_read(), _map(), _map("m2"), _rest_send()])
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(both_wrong, name="   ")
    assert exc.value.error_code.startswith("SYNC_PIPELINE")
    assert exc.value.field != "name"


# ---------------------------------------------------------------------------
# 8. Secret hygiene -- diagnostics are value-free
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c["source"].__setitem__("connection_id", ""),
        lambda c: c["transform"].__setitem__("map_ref", ""),
        lambda c: c["target"].__setitem__("dynamic_path", {"v": "SENTINEL-PATH"}),
        lambda c: c.__setitem__("secret_key", "SENTINEL-ROOT"),
    ],
)
def test_diagnostics_never_echo_an_authored_value(mutate):
    sentinels = (
        "SENTINEL-CONN", "SENTINEL-OP", "SENTINEL-MAP", "SENTINEL-LABEL",
        "SENTINEL-PATH", "SENTINEL-ROOT",
    )
    core = _core()
    core["source"].update(connection_id="SENTINEL-CONN", operation_id="SENTINEL-OP",
                          label="SENTINEL-LABEL")
    core["transform"]["map_ref"] = "SENTINEL-MAP"
    mutate(core)
    with pytest.raises(LegacyAdapterError) as exc:
        adapt_sync_pipeline(core)
    err = exc.value
    blob = "".join(
        [str(err), repr(err)] + [str(d) + repr(d) for d in err.diagnostics]
    )
    assert not [s for s in sentinels if s in blob], blob


# ---------------------------------------------------------------------------
# 9. The envelope survives the canonical arm
# ---------------------------------------------------------------------------


def test_envelope_description_folder_and_process_overrides_survive():
    cfg = _pipeline(
        [_rest_fetch(), _map(), _db_write()],
        description="A described sync",
        process_extensions={
            "connections": [
                {"connection_id": "TGT-CONN", "connector_type": "database",
                 "fields": [{"id": "url", "label": "URL", "xpath": "//url"}]}
            ]
        },
    )
    xml = SyncPipelineBuilder.build(cfg, name="Named", folder_name="Some/Folder")
    assert '<bns:description>A described sync</bns:description>' in xml
    assert 'folderName="Some/Folder"' in xml
    assert "<bns:processOverrides>" in xml
    assert "//url" in xml
    assert xml == ProcessFlowBuilder.build(
        SyncPipelineBuilder.lower_config(cfg), name="Named", folder_name="Some/Folder"
    )


def test_canonical_arm_uses_the_scheduled_process_options():
    """A listener process carries the 6-attribute listener option set; the
    canonical arm is non-listener by construction, so it must carry the
    7-attribute scheduled default including stopProcessingIfZeroDocuments.

    NOTE, because this reads as a contradiction against a live account: these are
    the bytes we EMIT. Boomi fills in its own default for the omitted attribute on
    save, so reading the persisted component back through the platform API shows
    ``stopProcessingIfZeroDocuments`` present on a listener too. Both are true --
    the listener option set is distinguished on the wire by ``allowSimultaneous``
    and ``updateRunDates``, which is what the assertions below pin.
    """
    canonical = SyncPipelineBuilder.build(_pipeline([_db_read(), _rest_send()]), name="P")
    listener = SyncPipelineBuilder.build(_pipeline([_listen(), _rest_send()]), name="P")
    assert "stopProcessingIfZeroDocuments" in canonical
    assert "stopProcessingIfZeroDocuments" not in listener
    assert 'allowSimultaneous="false"' in canonical and 'updateRunDates="true"' in canonical
    assert 'allowSimultaneous="true"' in listener and 'updateRunDates="false"' in listener


# ---------------------------------------------------------------------------
# 10. Darkness -- the adapter stays internal
# ---------------------------------------------------------------------------


def test_adapter_is_not_reexported_through_the_compiler_package():
    import boomi_mcp.compiler.process_ir as pir

    exported = set(getattr(pir, "__all__", ()))
    assert "adapt_sync_pipeline" not in exported
    assert "SYNC_PIPELINE_DIALECT" not in exported
    assert not [n for n in exported if "legacy_adapter" in n.lower()]
