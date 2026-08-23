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

WHO OWNS THE CASE DEFINITIONS (#165)
------------------------------------
This module OWNS every case definition, and the golden-producing test modules
consume them from here (each keeps a small alias block so its own assertions
and non-golden tests are untouched).  The direction used to be inverted — the
registry imported the producer test modules and called their helpers — which
meant deleting an owning test module made an otherwise-active golden
unrenderable: exactly the removal the ``transitional_oracle`` (#159) and
``deletion_only`` (#160) dispositions must survive.

The invariant, stated so it stays true as modules come and go: **no case input
is defined in a test module.**  Deliberately NOT stated as a module count —
#165's own issue text says "thirteen", which was the pre-inversion registry's
twelve direct imports plus one transitive, and no hand-typed tally survives the
next slice that adds or retires a module.  The checkable forms are: this file
imports zero test modules, and ``test_wave_gate_goldens.py`` renders every
active golden in a child where every ``test_*`` module is unimportable.

Where a case input already lives in a committed JSON fixture
(``tests/fixtures/process_ir/sync_pipeline_emitter_parity_cases.json``, the
``process_ir/rich_control`` and ``process_ir/error_handling`` documents, the
``recipe_parity`` baseline spec dumps, ``examples/m11``), the JSON is the
definition and this module is its sole Python binding; a test module's
in-Python copy is a WITNESS, pinned by that module's own fixture-equality
test.  Import cost is measured, not estimated: see MEASURED-IMPORT-COST below.

MEASURED-IMPORT-COST
--------------------
Bare ``import _wave_gate_golden_corpus`` in a fresh child process: 0.02-0.03 s
wall (three runs; ~0.010 s of it is the import itself per ``-X importtime``).
All production imports are lazy inside the case factories, so the bare import
stays flat.  A full ``--render`` of all 60 active cases in one child: 0.43 s
wall, production builders included.  Measured 2026-08-16 on CPython 3.12; raw
outputs in ``docs/architecture/evidence/issue-165/measurements/``.

CONTRACT
--------
* ``CASE_REGISTRY`` is CLOSED: a manifest row names a key in this dict, never an
  arbitrary module or callable.  A row naming an unknown case is a hard error.
* ``render_golden_case`` returns RAW bytes.  No newline normalisation, no XML
  re-parse/re-serialise, no canonicalisation, and it never reads the expected
  file — a renderer that peeked at its own answer would prove nothing.
* No renderer hands a module-level container — or a container nested inside one
  — to production code by reference: shared inputs are COPIED first (deeply
  where they nest; a flat dict of scalars is equivalently served by
  ``dict(...)`` / ``{**...}``), so one case cannot perturb another through a
  mutated module-level fixture. Both halves are asserted rather than described,
  in ``test_wave_gate_goldens.py``: the ANTECEDENT (every argument a corpus
  function receives AND every value it returns during a full render is checked,
  by identity, against every container reachable from module state) and the
  CONSEQUENCE (rendering every case leaves module state unchanged). The
  consequence alone was green with the copies removed, which is why the
  antecedent exists. The antecedent's bound, measured rather than assumed: it
  sees the corpus's own function boundary, so 57 of 60 cases are covered (the
  three ``recipe:*`` cases call no corpus function), and state reaching
  production without crossing that boundary — or becoming module state during a
  render — is out of its reach. That is an accepted limitation, not tracked work:
  no such leak exists today, and every in-tree protective copy is individually pinned.
* Per-section import SPELLING is load-bearing.  This repo has a bare/``src.``
  dual-module hazard: importing the other spelling yields a DIFFERENT class
  object.  Each section below imports its builders with the SAME spelling the
  owning test module uses, so the golden pins the object the tests exercise.
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent


def _bootstrap_sys_path():
    """Make the repo importable exactly the way pytest makes it importable.

    pytest (rootdir mode, no ``__init__.py`` in ``tests/``) prepends each test
    file's own directory to ``sys.path``.  The render child runs OUTSIDE
    pytest, so it reproduces that itself: ``src`` for the bare spelling,
    the repo root for the ``src.``-prefixed spelling.
    """
    for candidate in (_ROOT / "src", _ROOT, _HERE, _HERE / "patterns"):
        text = str(candidate)
        if text not in sys.path:
            sys.path.insert(0, text)


_bootstrap_sys_path()

RENDERERS = ("process-component-v1", "process-xml-v1", "component-xml-v1")


# ---------------------------------------------------------------------------
# A. ProcessFlowBuilder cases (consumed by tests/test_process_flow_builder.py)
# ---------------------------------------------------------------------------

PFB_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
PFB_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
PFB_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
PFB_REST_OP_ID = "44444444-4444-4444-4444-444444444444"

PFB_DATAPROCESS_GROOVY_SCRIPT = (
    "import java.util.Properties;\n"
    "import java.io.InputStream;\n"
    "\n"
    "for( int i = 0; i < dataContext.getDataCount(); i++ ) {\n"
    "    InputStream is = dataContext.getStream(i);\n"
    "    Properties props = dataContext.getProperties(i);\n"
    "    dataContext.storeStream(is, props);\n"
    "}"
)

PFB_JSON_PROFILE_ID = "55555555-5555-5555-5555-555555555555"
PFB_XML_PROFILE_ID = "66666666-6666-6666-6666-666666666666"
PFB_JSON_LINK_NAME = "ArrayElement1 (Root/Object/samplearray/samplearray/ArrayElement1)"
PFB_XML_LINK_NAME = "Group (Envelope/Body/Groups/Group)"

PFB_DOCCACHE_ID = "8540619c-9f1e-4832-9b1a-5128c399aa52"

PFB_REST_CONN_ID_2 = "55555555-5555-5555-5555-555555555555"
PFB_REST_OP_ID_2 = "66666666-6666-6666-6666-666666666666"

PFB_REST_B_CONN_ID = "55555555-5555-5555-5555-555555555555"
PFB_REST_B_OP_ID = "66666666-6666-6666-6666-666666666666"
PFB_SEQ_GROOVY = "dataContext.storeStream(is, props);"


def pfb_base_config(**overrides):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": PFB_DB_CONN_ID,
            "operation_id": PFB_DB_OP_ID,
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": PFB_REST_CONN_ID,
            "operation_id": PFB_REST_OP_ID,
            "action_type": "POST",
        },
    }
    cfg.update(overrides)
    return cfg


def pfb_dataprocess_config(steps=None, label="Tag documents", **overrides):
    transform = {"mode": "dataprocess", "label": label}
    transform["steps"] = (
        steps
        if steps is not None
        else [{"operation": "custom_scripting", "script": PFB_DATAPROCESS_GROOVY_SCRIPT}]
    )
    return pfb_base_config(transform=transform, **overrides)


def pfb_split_step(profile_type="json", profile_id=None, key=None, name=None, **extra):
    step = {
        "operation": "split_documents",
        "profile_type": profile_type,
        "profile_id": profile_id
        or (PFB_JSON_PROFILE_ID if profile_type == "json" else PFB_XML_PROFILE_ID),
        "link_element_key": key or ("9" if profile_type == "json" else "4"),
        "link_element_name": name
        or (PFB_JSON_LINK_NAME if profile_type == "json" else PFB_XML_LINK_NAME),
    }
    step.update(extra)
    return step


def pfb_combine_step(profile_type="json", profile_id=None, key=None, name=None, **extra):
    step = {
        "operation": "combine_documents",
        "profile_type": profile_type,
        "profile_id": profile_id
        or (PFB_JSON_PROFILE_ID if profile_type == "json" else PFB_XML_PROFILE_ID),
        "link_element_key": key or ("9" if profile_type == "json" else "4"),
        "link_element_name": name
        or (PFB_JSON_LINK_NAME if profile_type == "json" else PFB_XML_LINK_NAME),
    }
    step.update(extra)
    return step


def pfb_doccacheretrieve_config(label="Get Status Updates From Cache", **overrides):
    transform = {"mode": "doccacheretrieve", "document_cache_id": PFB_DOCCACHE_ID}
    if label is not None:
        transform["label"] = label
    transform.update(overrides.pop("transform_extra", {}))
    return pfb_base_config(transform=transform, **overrides)


def pfb_doccacheremove_config(label="Clear Status Cache", **overrides):
    transform = {"mode": "doccacheremove", "document_cache_id": PFB_DOCCACHE_ID}
    if label is not None:
        transform["label"] = label
    transform.update(overrides.pop("transform_extra", {}))
    return pfb_base_config(transform=transform, **overrides)


def pfb_branch_leg(connection_id=PFB_REST_CONN_ID_2, operation_id=PFB_REST_OP_ID_2,
                   action_type="PUT", **extra):
    leg = {
        "connector_type": "rest",
        "connection_id": connection_id,
        "operation_id": operation_id,
        "action_type": action_type,
    }
    leg.update(extra)
    return leg


def pfb_branch_config(targets=None, enabled=True, **overrides):
    branch = {"enabled": enabled,
              "targets": targets if targets is not None else [pfb_branch_leg()]}
    return pfb_base_config(branch=branch, **overrides)


def pfb_decision_block(**overrides):
    decision = {
        "comparison": "equals",
        "label": "Check Status",
        "left": {
            "value_type": "track",
            "property_id": "dynamicdocument.DDP_STATUS",
            "default_value": "",
            "property_name": "Dynamic Document Property - DDP_STATUS",
        },
        "right": {"value_type": "static", "static_value": "active"},
        "false_notify": "Decision false path: status was not active",
    }
    decision.update(overrides)
    return decision


def pfb_decision_config(decision=None, **overrides):
    return pfb_base_config(
        decision=decision if decision is not None else pfb_decision_block(), **overrides
    )


def pfb_flow_control_config(label="Batch by 10", for_each_count=10, **overrides):
    flow_control = {"enabled": True, "for_each_count": for_each_count}
    if label is not None:
        flow_control["label"] = label
    flow_control.update(overrides.pop("flow_control_extra", {}))
    return pfb_base_config(flow_control=flow_control, **overrides)


def pfb_rest_target(conn=PFB_REST_CONN_ID, op=PFB_REST_OP_ID, label="t", verb="POST"):
    return {
        "connector_type": "rest",
        "connection_id": conn,
        "operation_id": op,
        "action_type": verb,
        "label": label,
    }


def pfb_seq_config(flow_sequence, **overrides):
    return pfb_base_config(flow_sequence=flow_sequence, **overrides)


def pfb_decision_branch_config():
    """The canonical acceptance graph: Decision + Data Process on the true leg
    (-> top-level target) and a Branch whose legs each carry a Map (issue #117)."""
    return pfb_seq_config(
        [
            {
                "kind": "decision",
                "comparison": "equals",
                "left": {"value_type": "track", "property_id": "dynamicdocument.DDP_STATUS"},
                "right": {"value_type": "static", "static_value": "ACTIVE"},
                "label": "Status check",
                "true_steps": [
                    {
                        "kind": "dataprocess",
                        "label": "Tag",
                        "steps": [{"operation": "custom_scripting", "script": PFB_SEQ_GROOVY}],
                    }
                ],
                "false_steps": [
                    {
                        "kind": "branch",
                        "legs": [
                            {
                                "steps": [{"kind": "map_ref", "map_ref": "MAP-A", "label": "Map A"}],
                                "target": pfb_rest_target(label="Leg A"),
                            },
                            {
                                "steps": [{"kind": "map_ref", "map_ref": "MAP-B", "label": "Map B"}],
                                "target": pfb_rest_target(PFB_REST_B_CONN_ID, PFB_REST_B_OP_ID, "Leg B"),
                            },
                        ],
                    }
                ],
            }
        ]
    )


def pfb_cache_crud_config():
    return pfb_seq_config(
        [
            {"kind": "doccacheload", "document_cache_id": "CACHE-1", "label": "Add to cache"},
            {"kind": "doccacheretrieve", "document_cache_id": "CACHE-1", "label": "Read cache"},
            {"kind": "doccacheremove", "document_cache_id": "CACHE-1", "label": "Clear cache"},
        ]
    )


def pfb_exception_terminal_config():
    return pfb_seq_config(
        [
            {"kind": "message", "message_text": "processing", "label": "Log"},
            {
                "kind": "exception",
                "title": "Halt",
                "message_template": "halted: {1}",
                "parameter_source": "caught_error",
            },
        ]
    )


def pfb_set_properties_seq_config():
    """One set_ddp (static+profile+ddp sources) + one set_dpp (current+dpp,
    persisted) — the #121 golden graph."""
    return pfb_seq_config(
        [
            {
                "kind": "set_ddp",
                "name": "DDP_ORDER_PATH",
                "label": "Build order path",
                "source_values": [
                    {"value_type": "static", "value": "/orders/"},
                    {
                        "value_type": "profile",
                        "element_id": "7",
                        "element_name": "id (Root/Object/id)",
                        "profile_id": "77777777-7777-7777-7777-777777777777",
                        "profile_type": "profile.json",
                    },
                    {"value_type": "ddp", "property_name": "DDP_SUFFIX", "default_value": "0"},
                ],
            },
            {
                "kind": "set_dpp",
                "name": "DPP_LAST_PAYLOAD",
                "label": "Carry payload",
                "persist": True,
                "source_values": [
                    {"value_type": "current"},
                    {"value_type": "dpp", "property_name": "DPP_RUN_ID", "default_value": ""},
                ],
            },
        ]
    )


def pfb_cache_put_get_config():
    return pfb_seq_config(
        [
            {"kind": "cache_put", "document_cache_id": "CACHE-1", "label": "Stage rows"},
            {"kind": "cache_get", "document_cache_id": "CACHE-1", "label": "Read staged rows"},
        ]
    )


def _pfb_builder():
    # The ``src.`` spelling — the one tests/test_process_flow_builder.py uses.
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    return ProcessFlowBuilder


def _pfb_build(config, **kwargs):
    return _pfb_builder().build(config, **kwargs)


def _case_dataprocess_groovy():
    return _pfb_build(pfb_dataprocess_config(), name="DataProcess Groovy Sync")


def _case_dataprocess_split_json():
    return _pfb_build(
        pfb_dataprocess_config(steps=[pfb_split_step("json")], label="Split orders JSON"),
        name="DataProcess Split JSON Sync",
    )


def _case_dataprocess_split_xml():
    return _pfb_build(
        pfb_dataprocess_config(steps=[pfb_split_step("xml")], label="Split groups XML"),
        name="DataProcess Split XML Sync",
    )


def _case_dataprocess_combine_json():
    return _pfb_build(
        pfb_dataprocess_config(steps=[pfb_combine_step("json")], label="Combine orders JSON"),
        name="DataProcess Combine JSON Sync",
    )


def _case_dataprocess_combine_xml():
    return _pfb_build(
        pfb_dataprocess_config(steps=[pfb_combine_step("xml")], label="Combine groups XML"),
        name="DataProcess Combine XML Sync",
    )


def _case_document_cache_retrieve():
    return _pfb_build(pfb_doccacheretrieve_config(), name="DocumentCacheRetrieve Sync")


def _case_document_cache_remove():
    return _pfb_build(pfb_doccacheremove_config(), name="DocumentCacheRemove Sync")


def _case_return_documents_terminal():
    return _pfb_build(
        pfb_base_config(return_documents={"enabled": True}),
        name="Return Documents Subprocess",
    )


def _case_branch_fanout():
    return _pfb_build(
        pfb_branch_config(), name="Branch Fanout", folder_name="Golden/Fixtures"
    )


def _case_decision_conditional():
    return _pfb_build(
        pfb_decision_config(), name="Decision Conditional", folder_name="Golden/Fixtures"
    )


def _case_flow_control_batching():
    return _pfb_build(pfb_flow_control_config(), name="FlowControl Batching Sync")


def _case_flow_sequence_decision_branch_map():
    return _pfb_build(
        pfb_decision_branch_config(), name="Flow Sequence Decision Branch Map"
    )


def _case_flow_sequence_cache_crud():
    return _pfb_build(
        pfb_cache_crud_config(), name="Flow Sequence Cache Load Retrieve Remove"
    )


def _case_flow_sequence_exception_terminal():
    return _pfb_build(
        pfb_exception_terminal_config(), name="Flow Sequence Exception Terminal"
    )


def _case_set_properties_sequence():
    return _pfb_build(
        pfb_set_properties_seq_config(), name="Flow Sequence Set Properties DDP DPP"
    )


def _case_cache_put_get_sequence():
    return _pfb_build(pfb_cache_put_get_config(), name="Flow Sequence Cache Put Get")


# ---------------------------------------------------------------------------
# B. Dynamic path (consumed by tests/test_process_flow_builder_dynamic_path.py)
# ---------------------------------------------------------------------------

def dynpath_dynamic_path():
    return {
        "ddp_name": "DDP_PATH_CLIENTS",
        "request_profile_id": "PROFILE-UUID",
        "profile_type": "profile.json",
        "segments": [
            {"type": "static", "value": "/admin/cdscm/api/v1/clients/"},
            {
                "type": "profile",
                "element_id": 3,
                "element_name": "clientId (Root/Object/clientId)",
            },
        ],
    }


def dynpath_ddp_dynamic_path():
    # A ddp/dpp-only dynamic path: no profile segment -> no request_profile_id.
    return {
        "ddp_name": "DDP_PATH_ITEMS",
        "request_profile_id": None,
        "profile_type": None,
        "segments": [
            {"type": "static", "value": "/v1/items/"},
            {"type": "ddp", "property_name": "client_id"},
            {"type": "static", "value": "/notes/"},
            {"type": "dpp", "property_name": "run_id"},
        ],
    }


def dynpath_config(*, dynamic_path=None, reliability=None):
    target = {
        "connector_type": "rest",
        "action_type": "PATCH",
        "connection_id": "CONN-UUID",
        "operation_id": "OP-UUID",
    }
    if dynamic_path is not None:
        target["dynamic_path"] = dynamic_path
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "action_type": "Get",
            "connection_id": "DBCONN",
            "operation_id": "DBOP",
        },
        "transform": {"mode": "map_ref", "map_ref": "MAP-UUID"},
        "target": target,
    }
    if reliability is not None:
        cfg["reliability"] = reliability
    return cfg


def _dynpath_builder():
    # The bare spelling — the one tests/test_process_flow_builder_dynamic_path.py uses.
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        ProcessFlowBuilder,
    )

    return ProcessFlowBuilder


def _case_dynamic_path_target_profile():
    return _dynpath_builder().build(
        dynpath_config(dynamic_path=dynpath_dynamic_path()),
        name="Dynamic Path Profile Golden",
        folder_name="Golden/Fixtures",
    )


def _case_dynamic_path_source_ddp():
    return _dynpath_builder().build(
        dynpath_config(dynamic_path=dynpath_ddp_dynamic_path()),
        name="Dynamic Path DDP Golden",
        folder_name="Golden/Fixtures",
    )


# ---------------------------------------------------------------------------
# C. Scoped Try/Catch + DLQ (consumed by tests/test_process_flow_builder_trycatch_dlq.py)
# ---------------------------------------------------------------------------

DLQ_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
DLQ_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
DLQ_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
DLQ_REST_OP_ID = "44444444-4444-4444-4444-444444444444"
DLQ_CACHE_ID = "55555555-5555-5555-5555-555555555555"
DLQ_PROC_ID = "66666666-6666-6666-6666-666666666666"
DLQ_MAP_ID = "88888888-8888-8888-8888-888888888888"

# Issue #89: placeholder Notify config (references the caught-error property by
# its token; the builder substitutes it for the {1} placeholder + track param).
DLQ_NOTIFY_TOKEN = "meta.base.catcherrorsmessage"
DLQ_NOTIFY_TEMPLATE = (
    "Integration catch path failed. Caught error: " + DLQ_NOTIFY_TOKEN
)
DLQ_CATCH_NOTIFY = {"level": "ERROR", "message_template": DLQ_NOTIFY_TEMPLATE}


def dlq_config(dlq, transform=None, catch_notify=None):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": DLQ_DB_CONN_ID,
            "operation_id": DLQ_DB_OP_ID,
            "action_type": "Get",
            "label": "DB extract",
        },
        "transform": transform or {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": DLQ_REST_CONN_ID,
            "operation_id": DLQ_REST_OP_ID,
            "action_type": "POST",
            "label": "REST send",
        },
        "reliability": {"retry_count": 0, "dlq": dlq},
    }
    if catch_notify is not None:
        cfg["reliability"]["catch_notify"] = catch_notify
    return cfg


def dlq_connector_config(retry_count=2, transform=None, catch_notify=None, dlq=None):
    cfg = dlq_config(
        dlq or {"mode": "document_cache_ref", "document_cache_id": DLQ_CACHE_ID},
        transform=transform or {"mode": "map_ref", "map_ref": DLQ_MAP_ID},
        catch_notify=catch_notify,
    )
    cfg["reliability"]["retry_count"] = retry_count
    cfg["reliability"]["try_catch_scope"] = "connector"
    return cfg


def dlq_exc_config(catch_exception, dlq=None, catch_notify=None, retry_count=0,
                   scope="process"):
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": DLQ_DB_CONN_ID,
            "operation_id": DLQ_DB_OP_ID,
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": DLQ_REST_CONN_ID,
            "operation_id": DLQ_REST_OP_ID,
            "action_type": "POST",
        },
        "reliability": {
            "retry_count": retry_count,
            "try_catch_scope": scope,
            "catch_exception": catch_exception,
        },
    }
    if dlq is not None:
        cfg["reliability"]["dlq"] = dlq
    if catch_notify is not None:
        cfg["reliability"]["catch_notify"] = catch_notify
    return cfg


def _dlq_builder():
    # The ``src.`` spelling — the one tests/test_process_flow_builder_trycatch_dlq.py uses.
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    return ProcessFlowBuilder


def _case_try_catch_dlq_document_cache():
    cfg = dlq_config({"mode": "document_cache_ref", "document_cache_id": DLQ_CACHE_ID})
    return _dlq_builder().build(
        cfg, name="TryCatch DLQ Golden", folder_name="Golden/Fixtures"
    )


def _case_try_catch_dlq_retry_count_2():
    cfg = dlq_retry2_case_config()
    return _dlq_builder().build(
        cfg, name="TryCatch DLQ Retry2 Golden", folder_name="Golden/Fixtures"
    )


def dlq_retry2_case_config():
    """The retry_count=2 variant — ONE definition, consumed by both the corpus
    case above and the owning test's golden assertion."""
    cfg = dlq_config({"mode": "document_cache_ref", "document_cache_id": DLQ_CACHE_ID})
    cfg["reliability"]["retry_count"] = 2
    return cfg


def _case_try_catch_dlq_error_subprocess():
    cfg = dlq_config({"mode": "error_subprocess_ref", "process_id": DLQ_PROC_ID})
    return _dlq_builder().build(
        cfg, name="TryCatch Error Subprocess Golden", folder_name="Golden/Fixtures"
    )


def _case_try_catch_notify_dlq_document_cache():
    cfg = dlq_config(
        {"mode": "document_cache_ref", "document_cache_id": DLQ_CACHE_ID},
        catch_notify=copy.deepcopy(DLQ_CATCH_NOTIFY),
    )
    return _dlq_builder().build(
        cfg, name="TryCatch Notify DLQ Golden", folder_name="Golden/Fixtures"
    )


def _case_connector_scoped_trycatch_notify():
    cfg = dlq_connector_config(
        retry_count=2, catch_notify=copy.deepcopy(DLQ_CATCH_NOTIFY)
    )
    return _dlq_builder().build(
        cfg, name="Connector Scope DLQ Golden", folder_name="Golden/Fixtures"
    )


DLQ_EXCEPTION_CATCH = {
    "title": "Stopping - Throw Uncaught POST Error",
    "message_template": "Stopping process - uncaught error: {1}",
    "stop_single_document": False,
    "parameter_source": "caught_error",
}


def _case_exception_catch_path():
    cfg = dlq_exc_config(dict(DLQ_EXCEPTION_CATCH))
    return _dlq_builder().build(
        cfg, name="Exception Catch Path", folder_name="Golden/Fixtures"
    )


# ---------------------------------------------------------------------------
# D. Listener start (consumed by tests/test_process_flow_builder_listener.py)
# ---------------------------------------------------------------------------

def listener_pipeline_config(**listener_overrides):
    listener_config = {"primitive": "wss_listen", "operation_id": "WSSOP-1"}
    listener_config.update(listener_overrides)
    return {
        "process_kind": "sync_pipeline",
        "pipeline": {
            "stages": [
                {"key": "listen", "kind": "listener", "config": listener_config},
                {
                    "key": "map",
                    "kind": "map",
                    "config": {"primitive": "map", "map_ref": "MAP-1"},
                },
                {
                    "key": "send",
                    "kind": "send",
                    "config": {
                        "primitive": "rest_send",
                        "action_type": "POST",
                        "connection_id": "CONN-1",
                        "operation_id": "OP-1",
                    },
                },
            ],
            "dependencies": [
                {"from_stage": "listen", "to_stage": "map"},
                {"from_stage": "map", "to_stage": "send"},
            ],
        },
    }


def _sync_pipeline_builder():
    # The bare spelling — shared by the listener, adapter-cutover and parity tests.
    from boomi_mcp.categories.components.builders.process_flow_builder import (
        SyncPipelineBuilder,
    )

    return SyncPipelineBuilder


def _case_listener_wss_start():
    return _sync_pipeline_builder().build(
        listener_pipeline_config(),
        name="Listener WSS Start Golden",
        folder_name="Golden/Fixtures",
    )


# ---------------------------------------------------------------------------
# E. M11 composed examples (definitions live in examples/m11/*.json)
# ---------------------------------------------------------------------------

def load_m11_example(name):
    payload = json.loads(
        (_ROOT / "examples" / "m11" / name).read_text(encoding="utf-8")
    )
    assert payload["example_not_template"] is True
    assert payload["template_status"] == "example_only_not_reusable_template"
    return payload["integration_spec"]


def _case_m11_cache_property_basic():
    spec = load_m11_example("cache_property_authoring_basic.integration.json")
    process = next(c for c in spec["components"] if c["type"] == "process")
    # The ``src.`` spelling — the one tests/test_m11_composed_examples.py uses.
    from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder

    return ProcessFlowBuilder.build(process["config"], name=process["name"])


def _case_m11_processproperty_map_function():
    # The SAME import path the owning test uses (the bare/``src.`` dual-module
    # hazard: the other spelling is a DIFFERENT class object).
    from src.boomi_mcp.categories.components.builders.process_property_builder import (
        ProcessPropertyBuilder,
    )

    spec = load_m11_example("process_property_map_function.integration.json")
    prop = next(c for c in spec["components"] if c["type"] == "processproperty")
    return ProcessPropertyBuilder().build(**prop["config"])


# ---------------------------------------------------------------------------
# F. Wrapper subprocess (consumed by tests/test_wrapper_subprocess_builder.py)
# ---------------------------------------------------------------------------

WRAPPER_CHILD_ID = "11111111-1111-1111-1111-111111111111"


def wrapper_parent_case_config():
    return {
        "process_kind": "wrapper_subprocess",
        "process_calls": [
            {"subprocess_ref": WRAPPER_CHILD_ID, "wait": True, "abort_on_error": False,
             "label": "Run main-logic subprocess"},
        ],
    }


def _case_processcall_standalone_parent():
    # The ``src.`` spelling — the one tests/test_wrapper_subprocess_builder.py uses.
    from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder

    return WrapperSubprocessBuilder.build(
        wrapper_parent_case_config(),
        name="Wrapper Parent Golden",
        folder_name="Golden/Fixtures",
    )


# ---------------------------------------------------------------------------
# G. ProcessIR rich control bodies (definitions live in
#    tests/fixtures/process_ir/rich_control/*.json; the owning test module's
#    Python literals are witnesses pinned by its fixture-equality tests)
# ---------------------------------------------------------------------------

def rich_symbol(ref, component_type, **extra):
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1

    return ComponentSymbolV1(
        ref=ref, component_id="id_" + ref, component_type=component_type, **extra
    )


def rich_symbols():
    """Two connector families, each with its own map boundary."""
    from boomi_mcp.compiler.process_ir.contracts import SymbolTableV1

    return SymbolTableV1(
        symbols=(
            rich_symbol("conn_rest", "connector-settings", connector_type="rest"),
            rich_symbol("conn_soap", "connector-settings", connector_type="soap_client"),
            rich_symbol("conn_db", "connector-settings", connector_type="database"),
            rich_symbol("prof_rest_out", "profile.json"),
            rich_symbol("prof_soap_in", "profile.xml"),
            rich_symbol("prof_soap_out", "profile.xml"),
            rich_symbol("prof_db_write", "profile.db"),
            rich_symbol("prof_patch_in", "profile.json"),
            rich_symbol("prof_patch_out", "profile.json"),
            rich_symbol(
                "op_rest_get",
                "connector-action",
                connector_type="rest",
                action_type="GET",
                connection_ref="conn_rest",
                output_profile_ref="prof_rest_out",
            ),
            rich_symbol(
                "map_rest_to_soap",
                "transform.map",
                input_profile_ref="prof_rest_out",
                output_profile_ref="prof_soap_in",
            ),
            rich_symbol(
                "map_rest_to_patch",
                "transform.map",
                input_profile_ref="prof_rest_out",
                output_profile_ref="prof_patch_in",
            ),
            rich_symbol(
                "op_soap_execute",
                "connector-action",
                connector_type="soap_client",
                action_type="EXECUTE",
                connection_ref="conn_soap",
                input_profile_ref="prof_soap_in",
                output_profile_ref="prof_soap_out",
            ),
            rich_symbol(
                "op_rest_patch",
                "connector-action",
                connector_type="rest",
                action_type="PATCH",
                connection_ref="conn_rest",
                input_profile_ref="prof_patch_in",
                output_profile_ref="prof_patch_out",
            ),
            rich_symbol(
                "op_db_send",
                "connector-action",
                connector_type="database",
                action_type="Send",
                connection_ref="conn_db",
                input_profile_ref="prof_db_write",
            ),
            rich_symbol("child_process", "process"),
            # A real document cache. The one cache_get in this file used to name
            # `child_process` — a PROCESS component — which nothing checked until
            # #143 gated the compiler on the unified report.
            rich_symbol("doc_cache", "documentcache"),
        )
    )


def rich_compile_doc(doc, symbols=None, capabilities=None):
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    table = symbols or rich_symbols()
    return (
        compile_process_ir_v1(
            parse_process_ir_v1(doc), table, capabilities=capabilities
        ),
        table,
    )


def _canonical_envelope_case():
    """#153 (§6 review AR1-10): the canonical FULL COMPONENT ENVELOPE golden.

    Rendered through the PUBLIC chain — normalize -> build_materialization_plan
    (which owns compilation) -> late binding -> emit -> neutral materializer —
    with deterministic ids, so the frozen bytes pin the whole path and not just
    the emitter. The plan's golden-manifest requirement names exactly this
    artifact; the semantic-only ProcessIR goldens stay as they are.
    """
    def render():
        from boomi_mcp.authoring.contract import get_authoring_revisions
        from boomi_mcp.authoring.process_materialization import (
            build_materialization_plan,
        )
        from boomi_mcp.authoring.workflow import (
            _connector_metadata_from_components,
            _normalize_intent,
        )
        from boomi_mcp.categories.components.canonical_process_apply import (
            materialize_canonical_process_xml,
        )
        from boomi_mcp.categories.integration_builder import _materializer_revision
        from boomi_mcp.compiler.process_ir.emitter_registry import emitter_revision
        from boomi_mcp.models.authoring_workflow import (
            AuthoringRequestV1,
            ProcessIRAuthoringIntentV1,
        )
        from boomi_mcp.models.process_component import (
            ProcessAuthoringUnitV1,
            ProcessComponentEnvelopeV1,
        )
        from boomi_mcp.models.process_ir import parse_process_ir_v1
        from boomi_mcp.recipes.materialization import build_symbol_table

        # Inlined rather than imported from a test module: goldens must render
        # with every test module unimportable, and the corpus is the ONE
        # permitted exception. Mirrors tests/_m12_11_support's appliable
        # fixture; the two are pinned together by the goldens themselves — a
        # drift changes these bytes.
        conn = {
            "key": "conn", "type": "connector-settings", "name": "M12.15 conn",
            "action": "create",
            "config": {"connector_type": "rest", "component_name": "M12.15 conn",
                       "base_url": "https://orders.example.invalid", "auth": "NONE"},
        }
        op = {
            "key": "op", "type": "connector-action", "name": "M12.15 op",
            "action": "create", "depends_on": ["conn"],
            "config": {"connector_type": "rest", "operation_mode": "execute",
                       "component_name": "M12.15 op", "connection_ref_key": "conn",
                       "method": "GET", "path": "/v1/things"},
        }
        unit = ProcessAuthoringUnitV1(
            envelope=ProcessComponentEnvelopeV1(
                component_key="proc", name="M12.15 Process", action="create",
                depends_on=("conn", "op"),
            ),
            process_ir=parse_process_ir_v1({
                "version": "1",
                "body": {"kind": "sequence", "steps": [
                    {"kind": "source", "connection_ref": "$ref:conn",
                     "operation_ref": "$ref:op"},
                    {"kind": "message", "text": "hello"},
                    {"kind": "return_documents"},
                ]},
            }),
        )
        request = AuthoringRequestV1(
            intent=ProcessIRAuthoringIntentV1(
                integration_name="M12.15 Integration",
                units=(unit,),
                components=(conn, op),
            )
        )
        spec = _normalize_intent(request).integration_spec
        unit = spec.processes[0]
        symbols = build_symbol_table(
            list(spec.components),
            process_keys=[u.envelope.component_key for u in spec.processes],
            connector_metadata=_connector_metadata_from_components(spec.components),
        )
        plan = build_materialization_plan(
            envelope=unit.envelope,
            process_ir=unit.process_ir,
            symbols=symbols,
            conflict_policy="reuse",
            compiler_revision=get_authoring_revisions()["compiler_revision"],
            emitter_revision=emitter_revision(),
            materializer_revision=_materializer_revision(),
        )
        return materialize_canonical_process_xml(
            plan=plan,
            id_registry={"conn": "golden-conn-id", "op": "golden-op-id"},
            symbols=symbols,
        )
    return render


def _rich_case(fixture_name):
    def render():
        from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

        doc = json.loads(
            (_HERE / "fixtures" / "process_ir" / "rich_control" / fixture_name)
            .read_text(encoding="utf-8")
        )
        (_cfg, plan), table = rich_compile_doc(doc)
        return emit_process(plan, table).process_xml
    return render


# ---------------------------------------------------------------------------
# H. ProcessIR error handling anchors (definitions live in
#    tests/fixtures/process_ir/error_handling/*.json; the owning test module's
#    `_ANCHORS` literals are witnesses pinned by its fixture-equality test)
# ---------------------------------------------------------------------------

def error_symbols(*extra, contracts=()):
    from boomi_mcp.compiler.process_ir import connector_capabilities as CC
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )

    rest = CC.REST_FAMILY
    db = CC.DATABASE_FAMILY
    base = (
        ComponentSymbolV1(
            ref="$ref:GETOP",
            component_id="op-get",
            component_type="connector-action",
            connector_type=rest,
            action_type="GET",
            connection_ref="$ref:CONN",
            output_profile_ref="$ref:P1",
        ),
        ComponentSymbolV1(
            ref="$ref:GETOP2",
            component_id="op-get-2",
            component_type="connector-action",
            connector_type=rest,
            action_type="GET",
            connection_ref="$ref:CONN",
            output_profile_ref="$ref:P2",
        ),
        ComponentSymbolV1(
            ref="$ref:PATCHOP",
            component_id="op-patch",
            component_type="connector-action",
            connector_type=rest,
            action_type="PATCH",
            connection_ref="$ref:CONN",
            input_profile_ref="$ref:P1",
            output_profile_ref="$ref:P2",
        ),
        ComponentSymbolV1(
            ref="$ref:DBSEND",
            component_id="op-db-send",
            component_type="connector-action",
            connector_type=db,
            action_type="Send",
            connection_ref="$ref:DBCONN",
            input_profile_ref="$ref:P1",
        ),
        ComponentSymbolV1(
            ref="$ref:CONN",
            component_id="conn-1",
            component_type="connector-settings",
            connector_type=rest,
        ),
        ComponentSymbolV1(
            ref="$ref:DBCONN",
            component_id="conn-db",
            component_type="connector-settings",
            connector_type=db,
        ),
        ComponentSymbolV1(ref="$ref:P1", component_id="prof-1", component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:P2", component_id="prof-2", component_type="profile.json"),
        ComponentSymbolV1(ref="$ref:CACHE", component_id="cache-1", component_type="documentcache"),
    )
    return SymbolTableV1(symbols=base + tuple(extra), idempotency_contracts=tuple(contracts))


def error_compile(doc, symbols=None):
    from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    return compile_process_ir_v1(parse_process_ir_v1(doc), symbols or error_symbols())


#: #154 M12.16 — the six grammar shapes this slice makes expressible. The case
#: documents live in ``tests/fixtures/process_ir/issue154/*.json`` and their
#: per-shape oracle provenance is recorded in
#: ``tests/fixtures/process_ir/issue154/PROVENANCE.md``. They reuse
#: ``error_symbols`` because every shape is a connector flow with an
#: error-handling or terminal variation, which is exactly that table's subject.
def _issue154_case(name):
    def render():
        from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

        doc = json.loads(
            (_HERE / "fixtures" / "process_ir" / "issue154" / (name + ".json"))
            .read_text(encoding="utf-8")
        )
        symbols = error_symbols()
        _cfg, plan = error_compile(doc, symbols)
        return emit_process(plan, symbols).process_xml
    return render


def _error_case(anchor):
    def render():
        from boomi_mcp.compiler.process_ir.emitter_registry import emit_process

        doc = json.loads(
            (_HERE / "fixtures" / "process_ir" / "error_handling" / (anchor + ".json"))
            .read_text(encoding="utf-8")
        )
        symbols = error_symbols()
        _cfg, plan = error_compile(doc, symbols)
        return emit_process(plan, symbols).process_xml
    return render


# ---------------------------------------------------------------------------
# I. Typed recipe arms (definitions live in tests/fixtures/recipe_parity/*.json —
#    the baseline-captured IntegrationSpec dumps the owning test module pins
#    byte-identically with its L1 oracle)
# ---------------------------------------------------------------------------

#: Which recipe arm renders each golden case, and with which recipe id.
#: Dispatch below is TOTAL — an unknown arm raises rather than falling through
#: to a default. An `if arm == "fanout": … else: …` shape was the first cut and
#: it made only the "fanout" spelling load-bearing: mutating `"sync_preset"` to
#: any other string left all 60 goldens byte-identical, so a third arm typed in
#: here would have rendered silently through the sync-preset recipe.
RECIPE_GOLDEN_ARMS = {
    "compose_stream": "fanout",
    "compose_all_cache": "fanout",
    "api_to_api_sync_0": "sync_preset",
}


def _recipe_case(name):
    def render():
        from boomi_mcp.models.integration_models import IntegrationSpecV1
        from boomi_mcp.patterns.recipe_bridge import (
            run_fanout_recipe,
            run_sync_preset_recipe,
        )
        from boomi_mcp.recipes.builtins.catalog import (
            RECIPE_API_TO_API_SYNC,
            RECIPE_DB_REST_FANOUT,
        )

        payload = json.loads(
            (_HERE / "fixtures" / "recipe_parity" / (name + ".json"))
            .read_text(encoding="utf-8")
        )
        spec = IntegrationSpecV1.model_validate(payload)
        process = spec.components[-1]
        arms = {
            "fanout": (run_fanout_recipe, RECIPE_DB_REST_FANOUT),
            "sync_preset": (run_sync_preset_recipe, RECIPE_API_TO_API_SYNC),
        }
        arm = RECIPE_GOLDEN_ARMS[name]
        if arm not in arms:
            raise KeyError(
                "case {0!r} names recipe arm {1!r}, which is not one of {2}".format(
                    name, arm, sorted(arms)
                )
            )
        runner, recipe_id = arms[arm]
        result = runner(
            recipe_id=recipe_id, components=spec.components, process=process
        )
        return result.artifact_for(process.key).process_xml
    return render


# ---------------------------------------------------------------------------
# J. Archetype-path DLQ goldens (consumed by
#    tests/patterns/test_database_to_api_sync_dlq.py)
# ---------------------------------------------------------------------------

# Fixed ids for the archetype-path golden (mirror the builder golden's ids so
# the only structural delta is the archetype's map shape).
ARCH_DB_CONN_ID = "11111111-1111-1111-1111-111111111111"
ARCH_DB_OP_ID = "22222222-2222-2222-2222-222222222222"
ARCH_REST_CONN_ID = "33333333-3333-3333-3333-333333333333"
ARCH_REST_OP_ID = "44444444-4444-4444-4444-444444444444"
ARCH_CACHE_ID = "55555555-5555-5555-5555-555555555555"
ARCH_MAP_ID = "66666666-6666-6666-6666-666666666666"

# Issue #89: placeholder catch_notify (references the caught-error property).
ARCH_CATCH_NOTIFY = {
    "level": "ERROR",
    "message_template": "Sync failed; caught error: meta.base.catcherrorsmessage",
}

ARCH_WIRED_DC = {
    "enabled": True,
    "target": {"mode": "document_cache_ref", "document_cache_id": ARCH_CACHE_ID},
}


def arch_params(dlq=None, retry=None, catch_notify=None):
    """Smallest executable create/create payload; DLQ/retry/notify overridable."""
    reliability = {
        "retry": retry or {"max_attempts": 1},
        "dlq": dlq if dlq is not None else {"enabled": False},
        "error_classifier": {},
    }
    if catch_notify is not None:
        reliability["catch_notify"] = catch_notify
    return {
        "naming": {"integration_name": "demo-sync", "component_prefix": "DEMO"},
        "source": {
            "binding": {
                "mode": "create",
                "settings": {
                    "driver": "microsoft_jdbc",
                    "auth_mode": "username_password",
                    "host": "db.internal",
                    "database": "AppDB",
                    "username": "svc_sync",
                    "credential_ref": "secrets/db/svc_sync",
                },
            },
            "read_operation": {
                "sql": "<<user-authored DB read statement>>",
                "result_schema": {"fields": [{"name": "source_a", "data_type": "character"}]},
            },
        },
        "target": {
            "binding": {"mode": "create", "settings": {"base_url": "https://api.example.com", "auth_mode": "none"}},
            "send_request": {"method": "POST", "path": "/v1/items"},
            "payload_profile": {
                "format": "json",
                "root": {
                    "name": "Root",
                    "kind": "object",
                    "children": [{"name": "target_a", "kind": "simple", "data_type": "character"}],
                },
            },
        },
        "transform": {
            "operations": [
                {"operation_type": "direct", "source_field": "source_a", "target_path": "Root/target_a"}
            ]
        },
        "execution": {"trigger": {"mode": "manual"}},
        "reliability": reliability,
    }


def arch_result(dlq=None, retry=None, catch_notify=None):
    from boomi_mcp.categories.integration_authoring import build_from_archetype_action

    result = build_from_archetype_action(
        "database_to_api_sync", arch_params(dlq, retry, catch_notify)
    )
    assert result["_success"] is True, result
    return result


def arch_emit(dlq=None, retry=None, catch_notify=None):
    return arch_result(dlq, retry, catch_notify)["integration_spec"]


def arch_main_process(spec):
    return next(c for c in spec["components"] if c["type"] == "process")


def arch_build_archetype_process_xml(spec, name="Archetype DLQ Golden"):
    from boomi_mcp.categories.components.builders import ProcessFlowBuilder
    from boomi_mcp.categories.integration_builder import _resolve_dependency_tokens

    cfg = arch_main_process(spec)["config"]

    def _rk(token):
        return token[len("$ref:"):]

    registry = {
        _rk(cfg["source"]["connection_id"]): ARCH_DB_CONN_ID,
        _rk(cfg["source"]["operation_id"]): ARCH_DB_OP_ID,
        _rk(cfg["transform"]["map_ref"]): ARCH_MAP_ID,
        _rk(cfg["target"]["connection_id"]): ARCH_REST_CONN_ID,
        _rk(cfg["target"]["operation_id"]): ARCH_REST_OP_ID,
    }
    resolved = _resolve_dependency_tokens(cfg, registry)
    return ProcessFlowBuilder.build(resolved, name=name, folder_name="Golden/Fixtures")


def _case_archetype_dlq_document_cache():
    return arch_build_archetype_process_xml(arch_emit(copy.deepcopy(ARCH_WIRED_DC)))


def _case_archetype_notify_dlq_document_cache():
    spec = arch_emit(
        copy.deepcopy(ARCH_WIRED_DC), catch_notify=copy.deepcopy(ARCH_CATCH_NOTIFY)
    )
    return arch_build_archetype_process_xml(spec, name="Archetype Notify DLQ Golden")


# ---------------------------------------------------------------------------
# K. Legacy listener chains (consumed by tests/test_sync_pipeline_adapter_cutover.py)
# ---------------------------------------------------------------------------

CHAIN_TGT = {"connection_id": "TGT-CONN", "operation_id": "TGT-OP"}


def chain_stage(key, kind, defaults, over):
    """A stage whose config defaults every caller may override key-by-key."""
    config = dict(defaults)
    config.update(over)
    return {"key": key, "kind": kind, "config": config}


def chain_listen(key="s", **cfg):
    return chain_stage(
        key, "listener", {"primitive": "wss_listen", "operation_id": "WSSOP-1"}, cfg
    )


def chain_map(key="m", **cfg):
    return chain_stage(key, "map", {"primitive": "map", "map_ref": "MAP-1"}, cfg)


def chain_rest_send(key="t", **cfg):
    return chain_stage(
        key, "send", {"primitive": "rest_send", "action_type": "POST", **CHAIN_TGT}, cfg
    )


def chain_db_write(key="t", **cfg):
    return chain_stage(key, "write", {"primitive": "db_write", **CHAIN_TGT}, cfg)


# The four chains that stay on the legacy renderer (#140).
LISTENER_CHAINS = {
    "listener_send": [chain_listen(), chain_rest_send()],
    "listener_map_send": [chain_listen(), chain_map(), chain_rest_send()],
    "listener_write": [chain_listen(), chain_db_write()],
    "listener_map_write": [chain_listen(), chain_map(), chain_db_write()],
}


def listener_pipeline(stages, **top):
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


def listener_chain_golden_name(chain):
    """The ONE derivation of a listener chain's golden component name."""
    return (
        "Sync Listener "
        + chain.replace("listener_", "").replace("_", " ").title()
        + " Golden"
    )


def _listener_chain_case(chain):
    def render():
        cfg = listener_pipeline(copy.deepcopy(LISTENER_CHAINS[chain]))
        return _sync_pipeline_builder().build(
            cfg, name=listener_chain_golden_name(chain), folder_name="Golden/Fixtures"
        )
    return render


# ---------------------------------------------------------------------------
# L. The JSON-driven sync-pipeline parity corpus
# ---------------------------------------------------------------------------

def sync_parity_case(case_name):
    """One committed parity case: {config, name, folder_name, anchor}.

    The JSON corpus file is the definition; this accessor is its one Python
    binding, shared by the registry factory below and by
    ``tests/test_sync_pipeline_builder.py``'s golden assertions.  The config is
    deep-copied so no caller can perturb another through the parsed file.
    """
    corpus = json.loads(
        (_HERE / "fixtures" / "process_ir" / "sync_pipeline_emitter_parity_cases.json")
        .read_text(encoding="utf-8")
    )["cases"]
    return copy.deepcopy(corpus[case_name])


def _sync_parity_case(case_name):
    def render():
        case = sync_parity_case(case_name)
        return _sync_pipeline_builder().build(
            case["config"],
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
        # G0 — #153 canonical full component envelope, via the PUBLIC chain
        "canonical_envelope:appliable_rest": ("process-component-v1", _canonical_envelope_case()),
        # G — ProcessIR rich control bodies
        "process_ir_rich:branch_mixed_connectors": ("process-xml-v1", _rich_case("branch_mixed_connectors.json")),
        "process_ir_rich:decision_nested_bare_false_stop": ("process-xml-v1", _rich_case("decision_nested_bare_false_stop.json")),
        "process_ir_rich:branch_process_call": ("process-xml-v1", _rich_case("branch_process_call.json")),
        # H — ProcessIR error-handling anchors
        "process_ir_error:process_retry0_exception": ("process-xml-v1", _error_case("scoped_try_catch_process_retry0_exception")),
        "process_ir_error:connector_read_retry5_cache_catch": ("process-xml-v1", _error_case("scoped_try_catch_connector_read_retry5_cache_catch")),
        "process_ir_error:connector_read_to_connector_catch": ("process-xml-v1", _error_case("scoped_try_catch_connector_read_to_connector_catch")),
        # H2 — #154 M12.16 grammar widenings
        "issue154:try_flow_control": ("process-xml-v1", _issue154_case("try_flow_control")),
        "issue154:try_data_process": ("process-xml-v1", _issue154_case("try_data_process")),
        "issue154:try_return_documents": ("process-xml-v1", _issue154_case("try_return_documents")),
        "issue154:source_target_return_documents": ("process-xml-v1", _issue154_case("source_target_return_documents")),
        "issue154:catch_cache_put_exception": ("process-xml-v1", _issue154_case("catch_cache_put_exception")),
        "issue154:connector_linear_interleave": ("process-xml-v1", _issue154_case("connector_linear_interleave")),
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
    try:
        emitted = factory()
    except BaseException as exc:  # noqa: BLE001 - Skipped derives from BaseException
        # A renderer must RENDER. If a producer module's helper reaches
        # `pytest.skip()`, the per-commit golden test would report "skipped" and
        # the suite would stay green with that golden never rendered or
        # graph-verified — within the skip cap, invisibly. Converting it into a
        # hard failure keeps "every active golden ran" true in the ordinary suite
        # and not only under the explicit `wave` command.
        # Match the whole MRO, not the exact class: `pytest.xfail()` raises
        # `XFailed`, a SUBCLASS of `Failed`, which an exact-name comparison
        # misses — and an xfailed golden test is green, so the golden would go
        # unrendered with CI none the wiser. Checked by name rather than by
        # importing pytest, because this module also runs in a plain child
        # process outside pytest.
        # `unittest.SkipTest` too: pytest honours it as a skip, so re-raising it
        # left a golden unrendered while the run stayed green under the cap.
        _outcomes = {"Skipped", "SkipTest", "Failed", "XFailed", "OutcomeException",
                     "Exit"}
        if _outcomes.intersection(base.__name__ for base in type(exc).__mro__):
            raise RendererMismatch(
                "case {0!r} raised {1} instead of rendering; a golden renderer may "
                "not opt out — retire the row if the case is gone".format(
                    input_case, type(exc).__name__
                )
            )
        raise
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
