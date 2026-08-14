"""The closed golden-case registry: manifest row -> the bytes its producer emits.

Not a test module (underscore-prefixed), so pytest does not collect it.

WHY A REGISTRY AT ALL
---------------------
``scripts/wave_gate.py`` must render every ACTIVE golden-manifest entry TWICE,
in isolated child processes, and compare the bytes.  A pytest node id cannot
express that: the test that owns a golden may be deleted by #159/#160 while the
golden itself is still an ACTIVE ``transitional_oracle``.  Binding a manifest
row to a callable rather than to a test keeps the gate meaningful across that
transition — which is the entire point of the ``disposition`` axis.

WHY IT IMPORTS THE TEST MODULES
-------------------------------
Every case's configuration ALREADY exists, at module level, in the test module
that owns the golden (``_dataprocess_config``, ``LISTENER_CHAINS``, ``_ANCHORS``,
``SYNC_CASES``, ...).  This module calls those helpers instead of restating
their contents, so there is exactly ONE definition of every case input in the
tree.  Copying ~27 config literals here would have created a second copy that
drifts; refactoring thirteen high-value regression modules to source their
inputs from here would have put them on the operating table for no acceptance
criterion.  Importing costs 0.39 s for all thirteen modules (measured), paid
once per render child.

What IS restated here is only each case's invocation arguments — the component
name, the folder, which variant helper to call.  Those are pinned byte-for-byte
by the committed golden: get one wrong and the gate fails with GOLDEN_MISMATCH
immediately.  There is no silent-drift path.

CONTRACT
--------
* ``CASE_REGISTRY`` is CLOSED: a manifest row names a key in this dict, never an
  arbitrary module or callable.  A row naming an unknown case is a hard error.
* ``render_golden_case`` returns RAW bytes.  No newline normalisation, no XML
  re-parse/re-serialise, no canonicalisation, and it never reads the expected
  file — a renderer that peeked at its own answer would prove nothing.
* Every renderer deep-copies shared inputs, so one case cannot perturb another
  through a mutated module-level fixture.
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import importlib
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent


def _bootstrap_sys_path():
    """Make the repo importable exactly the way pytest makes it importable.

    pytest (rootdir mode, no ``__init__.py`` in ``tests/``) prepends each test
    file's own directory to ``sys.path``.  The render child runs OUTSIDE pytest,
    so it has to reproduce that itself or the producer modules' sibling imports
    (``from test_archetype_composition import ...``) fail.
    """
    for candidate in (_ROOT / "src", _ROOT, _HERE, _HERE / "patterns"):
        text = str(candidate)
        if text not in sys.path:
            sys.path.insert(0, text)


_bootstrap_sys_path()

RENDERERS = ("process-component-v1", "process-xml-v1", "component-xml-v1")

_MODULE_CACHE = {}


def _mod(name):
    """Import a producer module once per process."""
    if name not in _MODULE_CACHE:
        _MODULE_CACHE[name] = importlib.import_module(name)
    return _MODULE_CACHE[name]


# ---------------------------------------------------------------------------
# A. ProcessFlowBuilder cases owned by tests/test_process_flow_builder.py
# ---------------------------------------------------------------------------

def _pfb():
    return _mod("test_process_flow_builder")


def _pfb_build(config, **kwargs):
    return _pfb().ProcessFlowBuilder.build(config, **kwargs)


def _case_dataprocess_groovy():
    m = _pfb()
    return _pfb_build(m._dataprocess_config(), name="DataProcess Groovy Sync")


def _case_dataprocess_split_json():
    m = _pfb()
    return _pfb_build(
        m._dataprocess_config(steps=[m._split_step("json")], label="Split orders JSON"),
        name="DataProcess Split JSON Sync",
    )


def _case_dataprocess_split_xml():
    m = _pfb()
    return _pfb_build(
        m._dataprocess_config(steps=[m._split_step("xml")], label="Split groups XML"),
        name="DataProcess Split XML Sync",
    )


def _case_dataprocess_combine_json():
    m = _pfb()
    return _pfb_build(
        m._dataprocess_config(steps=[m._combine_step("json")], label="Combine orders JSON"),
        name="DataProcess Combine JSON Sync",
    )


def _case_dataprocess_combine_xml():
    m = _pfb()
    return _pfb_build(
        m._dataprocess_config(steps=[m._combine_step("xml")], label="Combine groups XML"),
        name="DataProcess Combine XML Sync",
    )


def _case_document_cache_retrieve():
    return _pfb_build(_pfb()._doccacheretrieve_config(), name="DocumentCacheRetrieve Sync")


def _case_document_cache_remove():
    return _pfb_build(_pfb()._doccacheremove_config(), name="DocumentCacheRemove Sync")


def _case_return_documents_terminal():
    m = _pfb()
    return _pfb_build(
        m._base_config(return_documents={"enabled": True}),
        name="Return Documents Subprocess",
    )


def _case_branch_fanout():
    return _pfb_build(
        _pfb()._branch_config(), name="Branch Fanout", folder_name="Golden/Fixtures"
    )


def _case_decision_conditional():
    return _pfb_build(
        _pfb()._decision_config(), name="Decision Conditional", folder_name="Golden/Fixtures"
    )


def _case_flow_control_batching():
    return _pfb_build(_pfb()._flow_control_config(), name="FlowControl Batching Sync")


def _case_flow_sequence_decision_branch_map():
    return _pfb_build(
        _pfb()._decision_branch_config(), name="Flow Sequence Decision Branch Map"
    )


def _case_flow_sequence_cache_crud():
    return _pfb_build(
        _pfb()._cache_crud_config(), name="Flow Sequence Cache Load Retrieve Remove"
    )


def _case_flow_sequence_exception_terminal():
    return _pfb_build(
        _pfb()._exception_terminal_config(), name="Flow Sequence Exception Terminal"
    )


def _case_set_properties_sequence():
    return _pfb_build(
        _pfb()._set_properties_seq_config(), name="Flow Sequence Set Properties DDP DPP"
    )


def _case_cache_put_get_sequence():
    return _pfb_build(_pfb()._cache_put_get_config(), name="Flow Sequence Cache Put Get")


# ---------------------------------------------------------------------------
# B. Dynamic path
# ---------------------------------------------------------------------------

def _case_dynamic_path_target_profile():
    m = _mod("test_process_flow_builder_dynamic_path")
    return m.ProcessFlowBuilder.build(
        m._config(dynamic_path=m._dynamic_path()),
        name="Dynamic Path Profile Golden",
        folder_name="Golden/Fixtures",
    )


def _case_dynamic_path_source_ddp():
    m = _mod("test_process_flow_builder_dynamic_path")
    return m.ProcessFlowBuilder.build(
        m._config(dynamic_path=m._ddp_dynamic_path()),
        name="Dynamic Path DDP Golden",
        folder_name="Golden/Fixtures",
    )


# ---------------------------------------------------------------------------
# C. Scoped Try/Catch + DLQ
# ---------------------------------------------------------------------------

def _dlq():
    return _mod("test_process_flow_builder_trycatch_dlq")


def _case_try_catch_dlq_document_cache():
    m = _dlq()
    cfg = m._config({"mode": "document_cache_ref", "document_cache_id": m._CACHE_ID})
    return m.ProcessFlowBuilder.build(
        cfg, name="TryCatch DLQ Golden", folder_name="Golden/Fixtures"
    )


def _case_try_catch_dlq_retry_count_2():
    m = _dlq()
    cfg = m._config({"mode": "document_cache_ref", "document_cache_id": m._CACHE_ID})
    cfg["reliability"]["retry_count"] = 2
    return m.ProcessFlowBuilder.build(
        cfg, name="TryCatch DLQ Retry2 Golden", folder_name="Golden/Fixtures"
    )


def _case_try_catch_dlq_error_subprocess():
    m = _dlq()
    cfg = m._config({"mode": "error_subprocess_ref", "process_id": m._PROC_ID})
    return m.ProcessFlowBuilder.build(
        cfg, name="TryCatch Error Subprocess Golden", folder_name="Golden/Fixtures"
    )


def _case_try_catch_notify_dlq_document_cache():
    m = _dlq()
    cfg = m._config(
        {"mode": "document_cache_ref", "document_cache_id": m._CACHE_ID},
        catch_notify=m._CATCH_NOTIFY,
    )
    return m.ProcessFlowBuilder.build(
        cfg, name="TryCatch Notify DLQ Golden", folder_name="Golden/Fixtures"
    )


def _case_connector_scoped_trycatch_notify():
    m = _dlq()
    cfg = m._connector_config(retry_count=2, catch_notify=m._CATCH_NOTIFY)
    return m.ProcessFlowBuilder.build(
        cfg, name="Connector Scope DLQ Golden", folder_name="Golden/Fixtures"
    )


def _case_exception_catch_path():
    m = _dlq()
    cfg = m._exc_config({
        "title": "Stopping - Throw Uncaught POST Error",
        "message_template": "Stopping process - uncaught error: {1}",
        "stop_single_document": False,
        "parameter_source": "caught_error",
    })
    return m.ProcessFlowBuilder.build(
        cfg, name="Exception Catch Path", folder_name="Golden/Fixtures"
    )


# ---------------------------------------------------------------------------
# D. Listener start (SyncPipelineBuilder)
# ---------------------------------------------------------------------------

def _case_listener_wss_start():
    m = _mod("test_process_flow_builder_listener")
    return m.SyncPipelineBuilder.build(
        m._listener_pipeline_config(),
        name="Listener WSS Start Golden",
        folder_name="Golden/Fixtures",
    )


# ---------------------------------------------------------------------------
# E. M11 composed examples
# ---------------------------------------------------------------------------

def _case_m11_cache_property_basic():
    m = _mod("test_m11_composed_examples")
    spec = m._load_example("cache_property_authoring_basic.integration.json")
    process = next(c for c in spec["components"] if c["type"] == "process")
    return m.ProcessFlowBuilder.build(process["config"], name=process["name"])


def _case_m11_processproperty_map_function():
    m = _mod("test_m11_composed_examples")
    # The SAME import path the owning test uses. This repo has a bare/``src.``
    # dual-module hazard: importing the other spelling yields a DIFFERENT class
    # object, and pinning the golden through one spelling while the test uses
    # the other would silently test two things.
    from src.boomi_mcp.categories.components.builders.process_property_builder import (
        ProcessPropertyBuilder,
    )

    spec = m._load_example("process_property_map_function.integration.json")
    prop = next(c for c in spec["components"] if c["type"] == "processproperty")
    return ProcessPropertyBuilder().build(**prop["config"])


# ---------------------------------------------------------------------------
# F. Wrapper subprocess
# ---------------------------------------------------------------------------

def _case_processcall_standalone_parent():
    m = _mod("test_wrapper_subprocess_builder")
    cfg = {
        "process_kind": "wrapper_subprocess",
        "process_calls": [
            {"subprocess_ref": m._CHILD_ID, "wait": True, "abort_on_error": False,
             "label": "Run main-logic subprocess"},
        ],
    }
    return m.WrapperSubprocessBuilder.build(
        cfg, name="Wrapper Parent Golden", folder_name="Golden/Fixtures"
    )


# ---------------------------------------------------------------------------
# G. ProcessIR rich control bodies (bare process XML)
# ---------------------------------------------------------------------------

def _rich_case(attribute):
    def render():
        m = _mod("test_process_ir_rich_control_bodies")
        doc = copy.deepcopy(getattr(m, attribute))
        (_cfg, plan), table = m.compile_doc(doc)
        return m.emit_process(plan, table).process_xml
    return render


# ---------------------------------------------------------------------------
# H. ProcessIR error handling anchors (bare process XML)
# ---------------------------------------------------------------------------

def _error_case(anchor):
    def render():
        m = _mod("test_process_ir_error_handling")
        doc = copy.deepcopy(dict(m._ANCHORS)[anchor])
        symbols = m._symbols()
        _cfg, plan = m._compile(doc, symbols)
        return m.emit_process(plan, symbols).process_xml
    return render


# ---------------------------------------------------------------------------
# I. Typed recipe arms (bare process XML)
# ---------------------------------------------------------------------------

def _recipe_case(name):
    def render():
        m = _mod("test_recipe_preset_parity")
        if name in m._COMPOSE_CASES:
            spec, process = m._spec_and_process(m._compose(name)["integration_spec"])
            result = m.run_fanout_recipe(
                recipe_id=m.RECIPE_DB_REST_FANOUT,
                components=spec.components,
                process=process,
            )
        else:
            spec, process = m._spec_and_process(m._build_preset(name)["integration_spec"])
            result = m.run_sync_preset_recipe(
                recipe_id=m.RECIPE_API_TO_API_SYNC,
                components=spec.components,
                process=process,
            )
        return result.artifact_for(process.key).process_xml
    return render


# ---------------------------------------------------------------------------
# J. Archetype-path DLQ goldens
# ---------------------------------------------------------------------------

def _case_archetype_dlq_document_cache():
    m = _mod("test_database_to_api_sync_dlq")
    return m._build_archetype_process_xml(m._emit(copy.deepcopy(m._WIRED_DC)))


def _case_archetype_notify_dlq_document_cache():
    m = _mod("test_database_to_api_sync_dlq")
    spec = m._emit(copy.deepcopy(m._WIRED_DC), catch_notify=m._CATCH_NOTIFY)
    return m._build_archetype_process_xml(spec, name="Archetype Notify DLQ Golden")


# ---------------------------------------------------------------------------
# K. Legacy listener chains (SyncPipelineBuilder)
# ---------------------------------------------------------------------------

def _listener_chain_case(chain):
    def render():
        m = _mod("test_sync_pipeline_adapter_cutover")
        name = (
            "Sync Listener "
            + chain.replace("listener_", "").replace("_", " ").title()
            + " Golden"
        )
        cfg = m._pipeline(copy.deepcopy(m.LISTENER_CHAINS[chain]))
        return m.SyncPipelineBuilder.build(cfg, name=name, folder_name="Golden/Fixtures")
    return render


# ---------------------------------------------------------------------------
# L. The JSON-driven sync-pipeline parity corpus
# ---------------------------------------------------------------------------

def _sync_parity_case(case_name):
    def render():
        m = _mod("test_process_emitter_parity")
        case = m.SYNC_CASES[case_name]
        return m.SyncPipelineBuilder.build(
            copy.deepcopy(case["config"]),
            name=case["name"],
            folder_name=case["folder_name"],
        )
    return render


def _sync_parity_entries():
    """One entry per ANCHORED case in the committed parity corpus.

    Derived from the corpus file rather than hand-listed: the corpus already
    records which case owns which anchor, and a second hand-maintained copy of
    that mapping is precisely the drift this programme keeps finding.
    """
    corpus = json.loads(
        (_HERE / "fixtures" / "process_ir" / "sync_pipeline_emitter_parity_cases.json")
        .read_text(encoding="utf-8")
    )["cases"]
    entries = {}
    for case_name in sorted(corpus):
        if not corpus[case_name].get("anchor"):
            continue
        entries["sync_parity:" + case_name] = (
            "process-component-v1", _sync_parity_case(case_name)
        )
    return entries


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------

def _build_registry():
    registry = {
        # A — ProcessFlowBuilder, complete Process components
        "process_flow:dataprocess_groovy": ("process-component-v1", _case_dataprocess_groovy),
        "process_flow:dataprocess_split_json": ("process-component-v1", _case_dataprocess_split_json),
        "process_flow:dataprocess_split_xml": ("process-component-v1", _case_dataprocess_split_xml),
        "process_flow:dataprocess_combine_json": ("process-component-v1", _case_dataprocess_combine_json),
        "process_flow:dataprocess_combine_xml": ("process-component-v1", _case_dataprocess_combine_xml),
        "process_flow:document_cache_retrieve": ("process-component-v1", _case_document_cache_retrieve),
        "process_flow:document_cache_remove": ("process-component-v1", _case_document_cache_remove),
        "process_flow:return_documents_terminal": ("process-component-v1", _case_return_documents_terminal),
        "process_flow:branch_fanout": ("process-component-v1", _case_branch_fanout),
        "process_flow:decision_conditional": ("process-component-v1", _case_decision_conditional),
        "process_flow:flow_control_batching": ("process-component-v1", _case_flow_control_batching),
        "process_flow:flow_sequence_decision_branch_map": ("process-component-v1", _case_flow_sequence_decision_branch_map),
        "process_flow:flow_sequence_cache_load_retrieve_remove": ("process-component-v1", _case_flow_sequence_cache_crud),
        "process_flow:flow_sequence_exception_terminal": ("process-component-v1", _case_flow_sequence_exception_terminal),
        "process_flow:set_properties_ddp_dpp": ("process-component-v1", _case_set_properties_sequence),
        "process_flow:flow_sequence_cache_put_get": ("process-component-v1", _case_cache_put_get_sequence),
        # B — dynamic path
        "dynamic_path:target_profile": ("process-component-v1", _case_dynamic_path_target_profile),
        "dynamic_path:source_ddp": ("process-component-v1", _case_dynamic_path_source_ddp),
        # C — scoped try/catch + DLQ
        "trycatch_dlq:document_cache": ("process-component-v1", _case_try_catch_dlq_document_cache),
        "trycatch_dlq:document_cache_retry2": ("process-component-v1", _case_try_catch_dlq_retry_count_2),
        "trycatch_dlq:error_subprocess": ("process-component-v1", _case_try_catch_dlq_error_subprocess),
        "trycatch_dlq:notify_document_cache": ("process-component-v1", _case_try_catch_notify_dlq_document_cache),
        "trycatch_dlq:connector_scope_notify": ("process-component-v1", _case_connector_scoped_trycatch_notify),
        "trycatch_dlq:exception_catch_path": ("process-component-v1", _case_exception_catch_path),
        # D — listener start
        "sync_pipeline:listener_wss_start": ("process-component-v1", _case_listener_wss_start),
        # E — M11 composed examples
        "m11:cache_property_basic": ("process-component-v1", _case_m11_cache_property_basic),
        "m11:processproperty_map_function": ("component-xml-v1", _case_m11_processproperty_map_function),
        # F — wrapper subprocess
        "wrapper_subprocess:standalone_parent": ("process-component-v1", _case_processcall_standalone_parent),
        # G — ProcessIR rich control bodies
        "process_ir_rich:branch_mixed_connectors": ("process-xml-v1", _rich_case("BRANCH_MIXED_DOC")),
        "process_ir_rich:decision_nested_bare_false_stop": ("process-xml-v1", _rich_case("DECISION_NESTED_DOC")),
        "process_ir_rich:branch_process_call": ("process-xml-v1", _rich_case("PROCESS_CALL_BRANCH_DOC")),
        # H — ProcessIR error-handling anchors
        "process_ir_error:process_retry0_exception": ("process-xml-v1", _error_case("scoped_try_catch_process_retry0_exception")),
        "process_ir_error:connector_read_retry5_cache_catch": ("process-xml-v1", _error_case("scoped_try_catch_connector_read_retry5_cache_catch")),
        "process_ir_error:connector_read_to_connector_catch": ("process-xml-v1", _error_case("scoped_try_catch_connector_read_to_connector_catch")),
        # I — typed recipe arms
        "recipe:compose_stream": ("process-xml-v1", _recipe_case("compose_stream")),
        "recipe:compose_all_cache": ("process-xml-v1", _recipe_case("compose_all_cache")),
        "recipe:api_to_api_sync_0": ("process-xml-v1", _recipe_case("api_to_api_sync_0")),
        # J — archetype-path DLQ
        "archetype_dlq:document_cache": ("process-component-v1", _case_archetype_dlq_document_cache),
        "archetype_dlq:notify_document_cache": ("process-component-v1", _case_archetype_notify_dlq_document_cache),
        # K — legacy listener chains
        "listener_chain:listener_send": ("process-component-v1", _listener_chain_case("listener_send")),
        "listener_chain:listener_map_send": ("process-component-v1", _listener_chain_case("listener_map_send")),
        "listener_chain:listener_write": ("process-component-v1", _listener_chain_case("listener_write")),
        "listener_chain:listener_map_write": ("process-component-v1", _listener_chain_case("listener_map_write")),
    }
    registry.update(_sync_parity_entries())
    return registry


CASE_REGISTRY = _build_registry()


class UnknownCase(KeyError):
    """The manifest names an input_case this registry does not define."""


class RendererMismatch(ValueError):
    """The manifest's renderer disagrees with the case's declared renderer."""


def declared_renderer(input_case):
    try:
        return CASE_REGISTRY[input_case][0]
    except KeyError:
        raise UnknownCase(input_case)


def render_golden_case(input_case, renderer):
    """Render one case and return its RAW bytes.

    Never reads the expected file, never normalises, never re-serialises.
    """
    try:
        declared, factory = CASE_REGISTRY[input_case]
    except KeyError:
        raise UnknownCase(input_case)
    if renderer != declared:
        raise RendererMismatch(
            "case {0!r} is a {1} case; the manifest says {2}".format(
                input_case, declared, renderer
            )
        )
    if renderer not in RENDERERS:  # pragma: no cover - closed by declared_renderer
        raise RendererMismatch("unknown renderer {0!r}".format(renderer))
    emitted = factory()
    if isinstance(emitted, bytes):
        return emitted
    if not isinstance(emitted, str):
        raise RendererMismatch(
            "case {0!r} produced {1}, not text".format(input_case, type(emitted).__name__)
        )
    return emitted.encode("utf-8")


# ---------------------------------------------------------------------------
# Child-process entry point used by scripts/wave_gate.py
# ---------------------------------------------------------------------------

def _main(argv=None):
    parser = argparse.ArgumentParser(description="render golden cases (machine use)")
    parser.add_argument(
        "--render", metavar="REQUEST_JSON", required=True,
        help="path to a JSON list of {id, input_case, renderer} objects",
    )
    args = parser.parse_args(argv)
    with open(args.render, "rb") as handle:
        request = json.loads(handle.read().decode("utf-8"))

    out = sys.stdout
    for item in request:
        payload = render_golden_case(item["input_case"], item["renderer"])
        out.write(json.dumps({
            "id": item["id"],
            "len": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "b64": base64.b64encode(payload).decode("ascii"),
        }, sort_keys=True) + "\n")
    out.flush()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(_main())
