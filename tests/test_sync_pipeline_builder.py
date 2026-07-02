"""Unit tests for the M5.2 sync_pipeline process builder (issue #70).

SyncPipelineBuilder lowers a verified-linear M5.1 PipelineSpec
(read(db_read) -> [map] -> send(rest_send)) into the proven
database_to_api_sync source/transform/target config and delegates XML
emission to ProcessFlowBuilder. It adds NO new shape: the emitted XML must be
byte-identical to the equivalent ProcessFlowBuilder output. Reserved stage
kinds, non-ordering edges, non-linear shapes, and the gated reliability/branch/
process_calls/return_documents blocks are rejected with structured errors.
"""

from __future__ import annotations

import pytest

from src.boomi_mcp.categories.components.builders import (
    ProcessFlowBuilder,
    SyncPipelineBuilder,
    PROCESS_FLOW_BUILDERS,
    get_process_flow_builder,
)
from src.boomi_mcp.categories.components.builders.connector_builder import (
    BuilderValidationError,
)


# ---------------------------------------------------------------------------
# Builders / fixtures
# ---------------------------------------------------------------------------

_DEPS = ["db_conn", "db_op", "field_map", "rest_conn", "rest_op"]


def _read_stage(key="source", **cfg):
    config = {"primitive": "db_read", "connection_id": "$ref:db_conn", "operation_id": "$ref:db_op"}
    config.update(cfg)
    return {"key": key, "kind": "read", "config": config}


def _map_stage(key="transform", **cfg):
    config = {"primitive": "map", "map_ref": "$ref:field_map"}
    config.update(cfg)
    return {"key": key, "kind": "map", "config": config}


def _send_stage(key="target", method="POST", **cfg):
    config = {
        "primitive": "rest_send",
        "action_type": method,
        "connection_id": "$ref:rest_conn",
        "operation_id": "$ref:rest_op",
    }
    config.update(cfg)
    return {"key": key, "kind": "send", "config": config}


# Issue #72 M5.4: a REST fetch source stage. Its $refs point at REST source
# connection/operation keys (distinct from the DB source keys above).
_FETCH_DEPS = ["rest_src_conn", "rest_src_op", "field_map", "rest_conn", "rest_op"]


def _fetch_stage(key="source", **cfg):
    config = {
        "primitive": "rest_fetch",
        "connection_id": "$ref:rest_src_conn",
        "operation_id": "$ref:rest_src_op",
    }
    config.update(cfg)
    return {"key": key, "kind": "fetch", "config": config}


# Issue #74 M5.8: a database write (db_write) target stage. Its $refs point at DB
# write connection/operation keys; the chain is fetch(rest_fetch) -> [map] ->
# write(db_write) (write is only supported from a REST fetch source).
_WRITE_DEPS = ["rest_src_conn", "rest_src_op", "field_map", "db_w_conn", "db_w_op"]


def _write_stage(key="target", **cfg):
    config = {
        "primitive": "db_write",
        "connection_id": "$ref:db_w_conn",
        "operation_id": "$ref:db_w_op",
    }
    config.update(cfg)
    return {"key": key, "kind": "write", "config": config}


def _sync_config(stages, dependencies, **top):
    cfg = {"process_kind": "sync_pipeline", "pipeline": {"stages": stages, "dependencies": dependencies}}
    cfg.update(top)
    return cfg


def _linear_with_map():
    return _sync_config(
        [_read_stage(), _map_stage(), _send_stage()],
        [
            {"from_stage": "source", "to_stage": "transform"},
            {"from_stage": "transform", "to_stage": "target"},
        ],
    )


def _linear_no_map(method="PATCH"):
    return _sync_config(
        [_read_stage("s"), _send_stage("t", method=method)],
        [{"from_stage": "s", "to_stage": "t"}],
    )


def _validate(cfg, depends_on=_DEPS):
    return SyncPipelineBuilder.validate_config(cfg, depends_on=depends_on)


def _code(cfg, depends_on=None):
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=depends_on)
    return err.error_code if err is not None else None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_registry_exposes_sync_pipeline():
    assert "sync_pipeline" in PROCESS_FLOW_BUILDERS
    assert get_process_flow_builder("sync_pipeline") is SyncPipelineBuilder
    # Case-insensitive lookup mirrors the other kinds.
    assert get_process_flow_builder("SYNC_PIPELINE") is SyncPipelineBuilder


def test_process_kind_constant():
    assert SyncPipelineBuilder.PROCESS_KIND == "sync_pipeline"
    assert issubclass(SyncPipelineBuilder, ProcessFlowBuilder)


def test_preservation_policy_owns_process_subtree():
    policy = SyncPipelineBuilder.PRESERVATION_POLICY
    assert policy.component_type == "process"
    assert any(p.path == "bns:object/process" for p in policy.owned_paths)


# ---------------------------------------------------------------------------
# Lowering equivalence with database_to_api_sync (core acceptance criterion)
# ---------------------------------------------------------------------------

# The database_to_api_sync archetype's _build_main_process linear core (minus
# the gated reliability / dynamic_path / folder_name blocks). Lowering a
# semantically-equivalent sync_pipeline must reproduce exactly this dict — that
# is "database_to_api_sync can be represented without changing public behavior".
_CORE_CONFIG = {
    "process_kind": "database_to_api_sync",
    "source": {
        "connector_type": "database",
        "action_type": "Get",
        "connection_id": "$ref:db_conn",
        "operation_id": "$ref:db_op",
    },
    "transform": {"mode": "map_ref", "map_ref": "$ref:field_map"},
    "target": {
        "connector_type": "rest",
        "action_type": "POST",
        "connection_id": "$ref:rest_conn",
        "operation_id": "$ref:rest_op",
    },
}


def test_lower_config_matches_database_to_api_sync_core():
    lowered = SyncPipelineBuilder.lower_config(_linear_with_map())
    assert lowered == _CORE_CONFIG


def test_lower_config_explicit_binding_fields_also_match():
    # Whether the read stage omits connector_type/action_type (semantic
    # shorthand) or states them explicitly, the lowering is identical.
    cfg = _sync_config(
        [
            _read_stage(connector_type="database", action_type="Get"),
            _map_stage(),
            _send_stage(connector_type="rest"),
        ],
        [
            {"from_stage": "source", "to_stage": "transform"},
            {"from_stage": "transform", "to_stage": "target"},
        ],
    )
    assert SyncPipelineBuilder.lower_config(cfg) == _CORE_CONFIG


def test_build_xml_equals_process_flow_builder_with_map():
    xml_sync = SyncPipelineBuilder.build(_linear_with_map(), name="Order Sync")
    xml_core = ProcessFlowBuilder.build(_CORE_CONFIG, name="Order Sync")
    assert xml_sync == xml_core


def test_build_xml_equals_process_flow_builder_passthrough():
    no_map = _linear_no_map(method="PATCH")
    lowered = SyncPipelineBuilder.lower_config(no_map)
    assert lowered["transform"] == {"mode": "passthrough"}
    expected_core = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "action_type": "Get",
            "connection_id": "$ref:db_conn",
            "operation_id": "$ref:db_op",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "action_type": "PATCH",
            "connection_id": "$ref:rest_conn",
            "operation_id": "$ref:rest_op",
        },
    }
    assert lowered == expected_core
    xml_sync = SyncPipelineBuilder.build(no_map, name="No Map")
    xml_core = ProcessFlowBuilder.build(expected_core, name="No Map")
    assert xml_sync == xml_core
    # Passthrough omits the map shape entirely: only the two connectoractions.
    assert 'shapetype="map"' not in xml_sync
    assert xml_sync.count('shapetype="connectoraction"') == 2


def test_injected_update_metadata_keys_are_tolerated():
    # The structured-update + safe-edit paths call build() with a payload that
    # carries injected component_type / component_name / name metadata (see
    # build_structured_update_xml). These must NOT be rejected — the base
    # process builders ignore unknown top-level keys, and sync_pipeline matches.
    payload = _linear_with_map()
    payload["component_type"] = "process"
    payload["component_name"] = "Sync Pipeline Process"
    payload["name"] = "Sync Pipeline Process"
    # lower_config tolerates them and produces the same lowered core config.
    assert SyncPipelineBuilder.lower_config(payload) == _CORE_CONFIG
    # build() (the update/safe-edit entry point) succeeds and matches the delegate.
    xml_sync = SyncPipelineBuilder.build(payload, name="Sync Pipeline Process")
    xml_core = ProcessFlowBuilder.build(_CORE_CONFIG, name="Sync Pipeline Process")
    assert xml_sync == xml_core


def test_build_carries_description_and_process_extensions():
    cfg = _linear_with_map()
    cfg["description"] = "Nightly order sync"
    cfg["process_extensions"] = {
        "connections": [
            {"connection_id": "$ref:db_conn", "fields": [{"id": "host", "label": "Host"}]}
        ]
    }
    lowered = SyncPipelineBuilder.lower_config(cfg)
    assert lowered["description"] == "Nightly order sync"
    assert lowered["process_extensions"] == cfg["process_extensions"]


# ---------------------------------------------------------------------------
# Valid-shape acceptance
# ---------------------------------------------------------------------------


def test_valid_with_map_validates_clean():
    assert _validate(_linear_with_map()) is None


def test_valid_no_map_validates_clean():
    assert _validate(_linear_no_map(method="POST")) is None


# ---------------------------------------------------------------------------
# REST fetch source stage (issue #72 M5.4)
# ---------------------------------------------------------------------------


def _fetch_with_map():
    return _sync_config(
        [_fetch_stage(), _map_stage(), _send_stage()],
        [
            {"from_stage": "source", "to_stage": "transform"},
            {"from_stage": "transform", "to_stage": "target"},
        ],
    )


def _fetch_no_map(method="POST"):
    return _sync_config(
        [_fetch_stage("s"), _send_stage("t", method=method)],
        [{"from_stage": "s", "to_stage": "t"}],
    )


def test_fetch_source_lowers_to_rest_get():
    lowered = SyncPipelineBuilder.lower_config(_fetch_with_map())
    assert lowered["source"] == {
        "connector_type": "rest",
        "action_type": "GET",
        "connection_id": "$ref:rest_src_conn",
        "operation_id": "$ref:rest_src_op",
    }
    assert lowered["transform"] == {"mode": "map_ref", "map_ref": "$ref:field_map"}


def test_fetch_with_map_validates_clean():
    assert _validate(_fetch_with_map(), depends_on=_FETCH_DEPS) is None


def test_fetch_no_map_validates_clean():
    assert _validate(_fetch_no_map(), depends_on=_FETCH_DEPS) is None


def test_fetch_source_build_emits_rest_get_canonical_subtype():
    xml = SyncPipelineBuilder.build(_fetch_no_map(), name="API Sync")
    # Two connectoractions (REST source + REST target), no map shape.
    assert xml.count('shapetype="connectoraction"') == 2
    assert 'shapetype="map"' not in xml
    # Source REST subtype kept mixed-case (the .lower() corruption guard) + GET.
    from src.boomi_mcp.categories.components.builders.connector_builder import (
        REST_CLIENT_SUBTYPE,
    )
    assert REST_CLIENT_SUBTYPE in xml
    assert REST_CLIENT_SUBTYPE.lower() not in xml
    assert 'actionType="GET"' in xml


def test_db_read_on_fetch_stage_rejected():
    # A fetch stage must declare rest_fetch, not db_read.
    cfg = _sync_config(
        [
            {"key": "s", "kind": "fetch", "config": {"primitive": "db_read",
             "connection_id": "$ref:rest_src_conn", "operation_id": "$ref:rest_src_op"}},
            _send_stage("t"),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_FETCH_DEPS)
    assert err.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"


def test_non_get_fetch_action_type_rejected():
    # rest_fetch is GET-only; a non-GET action_type is rejected (in lowering, so
    # the error fires for both validate_config and a direct build()).
    cfg = _sync_config(
        [_fetch_stage("s", action_type="POST"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_FETCH_DEPS)
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert err.field == "pipeline.stages[s].config.action_type"


def test_build_raises_on_non_get_fetch_bypass():
    # build() bypasses validate_config; the GET-only fetch constraint lives in
    # lowering, so a direct build() of a POST fetch source still fails cleanly
    # instead of emitting a REST source with actionType="POST".
    cfg = _sync_config(
        [_fetch_stage("s", action_type="POST"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(cfg, name="X")
    assert exc.value.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"


def test_explicit_null_fetch_action_type_resolves_to_get():
    # An explicit `action_type: null` on a fetch stage means "the default verb"
    # (GET) — identical to omitting it — so lowering yields GET and build() never
    # emits an empty actionType="" on the validate_config-bypass path.
    cfg = _sync_config(
        [_fetch_stage("s", action_type=None), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert SyncPipelineBuilder.lower_config(cfg)["source"]["action_type"] == "GET"
    assert SyncPipelineBuilder.validate_config(cfg, depends_on=_FETCH_DEPS) is None
    xml = SyncPipelineBuilder.build(cfg, name="X")
    assert 'actionType=""' not in xml
    assert 'actionType="GET"' in xml


def test_read_stage_connector_type_rest_rejected():
    # The read↔fetch split is not bypassable via a connector_type override: a read
    # stage forced to connector_type='rest' is rejected (use a fetch stage).
    cfg = _sync_config(
        [_read_stage("s", connector_type="rest", action_type="GET"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS)
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert err.field == "pipeline.stages[s].config.connector_type"


def test_fetch_stage_connector_type_database_rejected():
    # Symmetric guard: a fetch stage forced to connector_type='database' is rejected.
    cfg = _sync_config(
        [_fetch_stage("s", connector_type="database", action_type="Get"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_FETCH_DEPS)
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert err.field == "pipeline.stages[s].config.connector_type"


def test_read_stage_explicit_database_connector_type_still_accepted():
    # The legitimate explicit-but-matching case stays valid (read + 'database').
    cfg = _sync_config(
        [_read_stage("s", connector_type="database"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS) is None


# ---------------------------------------------------------------------------
# Reserved stage kinds / primitives  -> SYNC_PIPELINE_STAGE_UNSUPPORTED
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind,primitive,issue",
    [
        ("lookup", "lookup", None),
        ("combine", "combine", "#103"),
        ("flow_control", "flow_control", "#111"),
        ("decision", "decision", "#113"),
        ("dataprocess", "dataprocess", "#106"),
        ("exception", "exception", "#108"),
        ("doccacheretrieve", "doccacheretrieve", "#109"),
        ("doccacheremove", "doccacheremove", "#110"),
        ("branch", "branch", "#112"),
    ],
)
def test_reserved_stage_kind_rejected(kind, primitive, issue):
    cfg = _sync_config([{"key": "s", "kind": kind, "config": {"primitive": primitive}}], [])
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"
    if issue is not None:
        assert issue in (err.hint or "")


def test_reserved_primitive_on_read_stage_rejected():
    # A read stage that mis-declares the reserved rest_fetch primitive points at #72.
    cfg = _sync_config(
        [
            {"key": "s", "kind": "read", "config": {"primitive": "rest_fetch",
             "connection_id": "$ref:db_conn", "operation_id": "$ref:db_op"}},
            _send_stage("t"),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS)
    assert err.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"
    assert "#72" in (err.hint or "")


def test_reserved_primitive_on_send_stage_rejected():
    cfg = _sync_config(
        [
            _read_stage("s"),
            {"key": "t", "kind": "send", "config": {"primitive": "db_write",
             "action_type": "POST", "connection_id": "$ref:rest_conn", "operation_id": "$ref:rest_op"}},
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS)
    assert err.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"
    assert "#32" in (err.hint or "")


def test_missing_primitive_rejected_as_config_invalid():
    cfg = _sync_config(
        [
            {"key": "s", "kind": "read", "config": {"connection_id": "$ref:db_conn", "operation_id": "$ref:db_op"}},
            _send_stage("t"),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Non-linear / non-ordering shapes -> SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED
# ---------------------------------------------------------------------------


def test_non_ordering_edge_rejected():
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t", "edge_kind": "branch"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_fan_out_rejected():
    # read -> send AND read -> map: read fans out to two stages.
    cfg = _sync_config(
        [_read_stage("s"), _map_stage("m"), _send_stage("t")],
        [
            {"from_stage": "s", "to_stage": "t"},
            {"from_stage": "s", "to_stage": "m"},
        ],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_fan_in_rejected():
    # Two reads converging on one send (two start stages -> rejected).
    cfg = _sync_config(
        [_read_stage("s1"), _read_stage("s2"), _send_stage("t")],
        [
            {"from_stage": "s1", "to_stage": "t"},
            {"from_stage": "s2", "to_stage": "t"},
        ],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_disconnected_stage_rejected():
    # An orphan map stage not on the read->send path.
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t"), _map_stage("orphan")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_wrong_order_send_before_read_rejected():
    # send -> read: the chain's kinds are [send, read], not read->...->send.
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t")],
        [{"from_stage": "t", "to_stage": "s"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_two_maps_rejected():
    cfg = _sync_config(
        [_read_stage("s"), _map_stage("m1"), _map_stage("m2"), _send_stage("t")],
        [
            {"from_stage": "s", "to_stage": "m1"},
            {"from_stage": "m1", "to_stage": "m2"},
            {"from_stage": "m2", "to_stage": "t"},
        ],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_only_read_no_send_rejected():
    cfg = _sync_config([_read_stage("s")], [])
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_duplicate_stage_key_rejected():
    # PipelineSpec rejects duplicate keys -> surfaced as SYNC_PIPELINE_CONFIG_INVALID.
    cfg = _sync_config([_read_stage("s"), _send_stage("s")], [])
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Gated top-level blocks -> not silently dropped
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "block,value,code",
    [
        ("reliability", {"retry_count": 1}, "SYNC_PIPELINE_CONFIG_INVALID"),
        ("branch", {"enabled": True, "targets": []}, "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"),
        ("process_calls", [{"process_id": "x"}], "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"),
        ("return_documents", {"enabled": True}, "SYNC_PIPELINE_CONFIG_INVALID"),
        ("source", {"connector_type": "database"}, "SYNC_PIPELINE_CONFIG_INVALID"),
        ("target", {"connector_type": "rest"}, "SYNC_PIPELINE_CONFIG_INVALID"),
        ("transform", {"mode": "map_ref"}, "SYNC_PIPELINE_CONFIG_INVALID"),
    ],
)
def test_gated_top_level_block_rejected(block, value, code):
    cfg = _linear_with_map()
    cfg[block] = value
    assert _code(cfg, _DEPS) == code


@pytest.mark.parametrize("bad_key", ["reliabilty", "execution", "retries", "schedule"])
def test_unknown_top_level_key_rejected(bad_key):
    # A misspelled gated block (reliabilty) or an unsupported setting (execution)
    # must NOT be silently dropped — the verified-linear surface stays honest.
    cfg = _linear_with_map()
    cfg[bad_key] = {"whatever": True}
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert err.field == bad_key


def test_folder_id_rejected_steer_to_folder_name():
    # folder_id is NOT emitted by the process builder (only folderName is), so
    # accepting it would suppress FOLDER_REQUIRED_ON_CREATE while the component
    # still lands in root — reject it; placement goes through folder_name.
    cfg = _linear_with_map()
    cfg["folder_id"] = "some-folder-id"
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert err.field == "folder_id"


def test_folder_name_accepted():
    # folder_name IS emitted (folderName attr) — it stays allow-listed.
    cfg = _linear_with_map()
    cfg["folder_name"] = "Process Library/Sync"
    assert SyncPipelineBuilder.validate_config(cfg, depends_on=_DEPS) is None


def test_dynamic_path_in_send_stage_rejected():
    # A gated target sub-block must not be silently dropped into the lowered config.
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t", dynamic_path={"ddp_name": "X"})],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_unknown_config_key_in_stage_rejected():
    cfg = _sync_config(
        [_read_stage("s", surprise="x"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_runtime_bindings_in_send_stage_rejected():
    # Issue #96 M5.4a: the thin sync_pipeline stage cannot lower a runtime binding
    # (it has no operation path template) — a runtime_bindings key on a send stage
    # is a gated sub-block, rejected (not silently dropped). The binding is
    # expressed on the rest_send operation config instead.
    cfg = _sync_config(
        [
            _read_stage("s"),
            _send_stage("t", runtime_bindings=[
                {"location": "query_parameter", "slot": "x",
                 "source": {"kind": "static", "value": "1"}}
            ]),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_runtime_bindings_in_fetch_stage_rejected():
    cfg = _sync_config(
        [
            _fetch_stage("s", runtime_bindings=[
                {"location": "path", "slot": "id",
                 "source": {"kind": "dpp", "property_name": "last_id"}}
            ]),
            _send_stage("t"),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _FETCH_DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_component_ref_stage_rejected():
    cfg = _sync_config(
        [
            {"key": "s", "kind": "read", "component_ref": "some_existing"},
            _send_stage("t"),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Malformed config / pipeline
# ---------------------------------------------------------------------------


def test_missing_pipeline_rejected():
    assert _code({"process_kind": "sync_pipeline"}, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_non_dict_pipeline_rejected():
    assert _code({"process_kind": "sync_pipeline", "pipeline": []}, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_empty_stages_rejected():
    assert _code(_sync_config([], []), _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


def test_wrong_process_kind_rejected():
    cfg = _linear_with_map()
    cfg["process_kind"] = "database_to_api_sync"
    assert _code(cfg, _DEPS) == "PROCESS_KIND_UNSUPPORTED"


def test_map_stage_without_map_ref_rejected():
    cfg = _sync_config(
        [_read_stage("s"), {"key": "m", "kind": "map", "config": {"primitive": "map"}}, _send_stage("t")],
        [
            {"from_stage": "s", "to_stage": "m"},
            {"from_stage": "m", "to_stage": "t"},
        ],
    )
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# Delegated binding validation still applies through the lowered config
# ---------------------------------------------------------------------------


def test_missing_connection_id_rejected_by_delegate():
    cfg = _sync_config(
        [
            {"key": "s", "kind": "read", "config": {"primitive": "db_read", "operation_id": "$ref:db_op"}},
            _send_stage("t"),
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _code(cfg, _DEPS) == "PROCESS_CONNECTOR_BINDING_INVALID"


def test_send_without_action_type_rejected_by_lowering():
    # A send stage missing action_type is rejected in lowering (#128 C1), so the
    # error now originates in _lower_binding_stage — the precise stage field is
    # available — rather than in the delegate's _validate_target_binding. The
    # error code is unchanged (PROCESS_CONNECTOR_BINDING_INVALID).
    cfg = _sync_config(
        [
            _read_stage("s"),
            {"key": "t", "kind": "send", "config": {"primitive": "rest_send",
             "connection_id": "$ref:rest_conn", "operation_id": "$ref:rest_op"}},
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = _validate(cfg, depends_on=_DEPS)
    assert err is not None
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert err.field == "pipeline.stages[t].config.action_type"


def test_build_raises_on_send_without_action_type_bypass():
    # build() bypasses validate_config; the send non-empty action_type invariant
    # lives in lowering (#128 C1, mirror of the fetch GET-only / write Send-only
    # bypass guards), so a direct build() of a send stage with no action_type
    # fails cleanly instead of emitting a REST target with actionType="".
    cfg = _sync_config(
        [
            _read_stage("s"),
            {"key": "t", "kind": "send", "config": {"primitive": "rest_send",
             "connection_id": "$ref:rest_conn", "operation_id": "$ref:rest_op"}},
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(cfg, name="X")
    assert exc.value.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert exc.value.field == "pipeline.stages[t].config.action_type"


def test_send_explicit_null_action_type_rejected():
    # Unlike fetch (null -> GET) and write (null -> Send), a send target has no
    # default verb: an explicit action_type=None is rejected, not normalized.
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t", action_type=None)],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = _validate(cfg, depends_on=_DEPS)
    assert err is not None
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert err.field == "pipeline.stages[t].config.action_type"


def test_send_blank_action_type_rejected():
    # A whitespace-only action_type is empty after strip() and rejected too.
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t", action_type="   ")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = _validate(cfg, depends_on=_DEPS)
    assert err is not None
    assert err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert err.field == "pipeline.stages[t].config.action_type"


def test_unreachable_ref_rejected_by_delegate():
    # $ref:field_map not in depends_on -> MISSING_PROCESS_DEPENDENCY from the
    # delegated reachability walk over the lowered config.
    assert _code(_linear_with_map(), ["db_conn", "db_op", "rest_conn", "rest_op"]) == "MISSING_PROCESS_DEPENDENCY"


def test_plaintext_secret_in_stage_config_rejected():
    cfg = _sync_config(
        [_read_stage("s"), _send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    # Inject a secret-shaped key into a stage config (unknown key still scanned
    # by the inherited secret scan path at the integration layer; here we assert
    # the builder's own scan via validate_config delegate path is reachable).
    cfg["pipeline"]["stages"][0]["config"]["api_key"] = "sekret"
    # The stage allow-list rejects the unknown key first (config-invalid), which
    # is the correct gate — a secret-shaped key never reaches the lowered config.
    assert _code(cfg, _DEPS) == "SYNC_PIPELINE_CONFIG_INVALID"


# ---------------------------------------------------------------------------
# build() totality on a validate_config bypass
# ---------------------------------------------------------------------------


def test_build_raises_on_malformed_config_bypass():
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build({"process_kind": "sync_pipeline"}, name="X")
    assert exc.value.error_code == "SYNC_PIPELINE_CONFIG_INVALID"


def test_build_raises_on_reserved_kind_bypass():
    # 'lookup' remains a reserved kind (no PipelineSpec lowering) after write was
    # added in M5.8 (#74); a direct build() that bypasses validate_config still
    # fails cleanly instead of emitting a malformed process.
    cfg = _sync_config([{"key": "s", "kind": "lookup", "config": {"primitive": "lookup"}}], [])
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(cfg, name="X")
    assert exc.value.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"


# ---------------------------------------------------------------------------
# Issue #74 M5.8 — database write (db_write) target: fetch -> [map] -> write
# ---------------------------------------------------------------------------


def _fetch_to_write():
    return _sync_config(
        [_fetch_stage(), _map_stage(), _write_stage()],
        [
            {"from_stage": "source", "to_stage": "transform"},
            {"from_stage": "transform", "to_stage": "target"},
        ],
    )


def _fetch_to_write_no_map():
    return _sync_config(
        [_fetch_stage("s"), _write_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )


def test_write_target_lowers_to_database_send():
    lowered = SyncPipelineBuilder.lower_config(_fetch_to_write())
    assert lowered["target"] == {
        "connector_type": "database",
        "action_type": "Send",
        "connection_id": "$ref:db_w_conn",
        "operation_id": "$ref:db_w_op",
    }
    # The fetch source still lowers to a REST GET; the map is unchanged.
    assert lowered["source"]["connector_type"] == "rest"
    assert lowered["transform"] == {"mode": "map_ref", "map_ref": "$ref:field_map"}


def test_fetch_to_write_with_map_validates_clean():
    assert _validate(_fetch_to_write(), depends_on=_WRITE_DEPS) is None


def test_fetch_to_write_no_map_validates_clean():
    assert _validate(_fetch_to_write_no_map(), depends_on=_WRITE_DEPS) is None


def test_write_target_build_emits_database_send_connectoraction():
    xml = SyncPipelineBuilder.build(_fetch_to_write_no_map(), name="API to DB Sync")
    # Two connectoractions (REST source + DB target), no map shape.
    assert xml.count('shapetype="connectoraction"') == 2
    assert 'shapetype="map"' not in xml
    # The target connectoraction emits connectorType="database" actionType="Send"
    # (mixed-case Send preserved — not uppercased to SEND).
    assert 'connectorType="database"' in xml
    assert 'actionType="Send"' in xml
    assert 'actionType="SEND"' not in xml


def test_read_to_write_db_to_db_rejected():
    # A write target is only supported from a REST fetch source; a read(db_read) ->
    # write(db_write) DB-to-DB chain is out of scope and rejected.
    cfg = _sync_config(
        [_read_stage("s"), _write_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_WRITE_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONTROL_FLOW_UNSUPPORTED"


def test_rest_send_on_write_stage_rejected():
    # A write stage must declare db_write, not rest_send.
    cfg = _sync_config(
        [
            _fetch_stage("s"),
            {"key": "t", "kind": "write", "config": {"primitive": "rest_send",
             "action_type": "POST", "connection_id": "$ref:db_w_conn", "operation_id": "$ref:db_w_op"}},
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_WRITE_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_STAGE_UNSUPPORTED"


def test_send_stage_forced_to_database_connector_type_rejected():
    # A send stage carries the rest_send primitive but an explicit
    # connector_type='database' would lower to a DB target — the target-family
    # guard rejects the contradiction (mirrors the read/fetch source guard) so
    # the send-vs-write split cannot be bypassed.
    cfg = _sync_config(
        [
            _fetch_stage("s"),
            {"key": "t", "kind": "send", "config": {"primitive": "rest_send",
             "connector_type": "database", "action_type": "Send",
             "connection_id": "$ref:db_w_conn", "operation_id": "$ref:db_w_op"}},
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_WRITE_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert "send stage" in str(err)


def test_build_raises_on_non_send_write_bypass():
    # build() bypasses validate_config; the Send-only db_write constraint lives in
    # lowering, so a direct build() of a write stage with an explicit wrong
    # action_type still fails cleanly instead of emitting a database target step
    # with actionType="Get" (#74 review — mirror of the fetch GET-only bypass guard).
    cfg = _sync_config(
        [_fetch_stage("s"), _write_stage("t", action_type="Get")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    with pytest.raises(BuilderValidationError) as exc:
        SyncPipelineBuilder.build(cfg, name="X")
    assert exc.value.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"


def test_explicit_send_write_action_type_accepted():
    # An explicit action_type='Send' on a write stage is identical to omitting it
    # (the default) — lowering accepts it and validates clean.
    cfg = _sync_config(
        [_fetch_stage("s"), _write_stage("t", action_type="Send")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert _validate(cfg, depends_on=_WRITE_DEPS) is None
    lowered = SyncPipelineBuilder.lower_config(cfg)
    assert lowered["target"]["action_type"] == "Send"


def test_explicit_null_write_action_type_resolves_to_send():
    # An explicit action_type=None on a write stage means "the default verb"
    # (Send) — identical to omitting the key — so lowering resolves it to Send and
    # build() never leaks actionType="" (#128 C4, mirror of the fetch null->GET
    # normalization at test_explicit_null_fetch_action_type_resolves_to_get).
    cfg = _sync_config(
        [_fetch_stage("s"), _write_stage("t", action_type=None)],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    assert SyncPipelineBuilder.lower_config(cfg)["target"]["action_type"] == "Send"
    assert _validate(cfg, depends_on=_WRITE_DEPS) is None
    xml = SyncPipelineBuilder.build(cfg, name="X")
    assert 'actionType=""' not in xml
    assert 'actionType="Send"' in xml


def test_write_stage_forced_to_rest_connector_type_rejected():
    # Symmetrically, a write stage forced to connector_type='rest' (which would
    # lower to a REST target) is rejected — a write stage is database-only.
    cfg = _sync_config(
        [
            _fetch_stage("s"),
            {"key": "t", "kind": "write", "config": {"primitive": "db_write",
             "connector_type": "rest", "action_type": "POST",
             "connection_id": "$ref:db_w_conn", "operation_id": "$ref:db_w_op"}},
        ],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=_WRITE_DEPS)
    assert err is not None
    assert err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"
    assert "write stage" in str(err)


# ---------------------------------------------------------------------------
# Issue #126 M5.10 — SOAP Client fetch/send stages
# ---------------------------------------------------------------------------

_SOAP_FETCH_DEPS = ["soap_src_conn", "soap_src_op", "field_map", "rest_conn", "rest_op"]
_SOAP_SEND_DEPS = ["rest_src_conn", "rest_src_op", "field_map", "soap_conn", "soap_op"]


def _soap_fetch_stage(key="source", **cfg):
    config = {
        "primitive": "soap_fetch",
        "connection_id": "$ref:soap_src_conn",
        "operation_id": "$ref:soap_src_op",
    }
    config.update(cfg)
    return {"key": key, "kind": "fetch", "config": config}


def _soap_send_stage(key="target", **cfg):
    config = {
        "primitive": "soap_send",
        "connection_id": "$ref:soap_conn",
        "operation_id": "$ref:soap_op",
    }
    config.update(cfg)
    return {"key": key, "kind": "send", "config": config}


def test_soap_fetch_source_lowers_to_soap_execute():
    cfg = _sync_config(
        [_soap_fetch_stage("s"), _send_stage("t", method="POST")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["soap_src_conn", "soap_src_op", "rest_conn", "rest_op"]
    assert SyncPipelineBuilder.validate_config(cfg, depends_on=deps) is None
    lowered = SyncPipelineBuilder.lower_config(cfg)
    assert lowered["source"]["connector_type"] == "soap_client"
    assert lowered["source"]["action_type"] == "EXECUTE"


def test_soap_send_target_lowers_to_soap_execute():
    cfg = _sync_config(
        [_read_stage("s"), _soap_send_stage("t")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["db_conn", "db_op", "soap_conn", "soap_op"]
    assert SyncPipelineBuilder.validate_config(cfg, depends_on=deps) is None
    lowered = SyncPipelineBuilder.lower_config(cfg)
    assert lowered["target"]["connector_type"] == "soap_client"
    assert lowered["target"]["action_type"] == "EXECUTE"


def test_soap_fetch_to_soap_send_full_chain_emits_wssoapclientsdk():
    cfg = _sync_config(
        [_soap_fetch_stage("s"), _map_stage("m"), _soap_send_stage("t")],
        [{"from_stage": "s", "to_stage": "m"}, {"from_stage": "m", "to_stage": "t"}],
    )
    deps = ["soap_src_conn", "soap_src_op", "field_map", "soap_conn", "soap_op"]
    assert SyncPipelineBuilder.validate_config(cfg, depends_on=deps) is None
    xml = SyncPipelineBuilder.build(cfg, name="SOAP Sync", folder_name="Test")
    assert xml.count('connectorType="wssoapclientsdk"') == 2
    assert xml.count('actionType="EXECUTE"') == 2


def test_soap_fetch_defaults_execute_action_type():
    # action_type omitted -> defaults to EXECUTE for a soap_fetch source.
    cfg = _sync_config(
        [_soap_fetch_stage("s"), _send_stage("t", method="POST")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["soap_src_conn", "soap_src_op", "rest_conn", "rest_op"]
    lowered = SyncPipelineBuilder.lower_config(cfg)
    assert lowered["source"]["action_type"] == "EXECUTE"


def test_soap_fetch_rejects_non_execute_action():
    cfg = _sync_config(
        [_soap_fetch_stage("s", action_type="GET"), _send_stage("t", method="POST")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["soap_src_conn", "soap_src_op", "rest_conn", "rest_op"]
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=deps)
    assert err is not None and err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert "EXECUTE" in str(err)


def test_soap_send_rejects_non_execute_action():
    cfg = _sync_config(
        [_read_stage("s"), _soap_send_stage("t", action_type="POST")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["db_conn", "db_op", "soap_conn", "soap_op"]
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=deps)
    assert err is not None and err.error_code == "PROCESS_CONNECTOR_BINDING_INVALID"
    assert "EXECUTE" in str(err)


def test_soap_fetch_primitive_with_rest_connector_type_rejected():
    # A soap_fetch primitive whose explicit connector_type is REST is rejected.
    cfg = _sync_config(
        [_soap_fetch_stage("s", connector_type="rest"), _send_stage("t", method="POST")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["soap_src_conn", "soap_src_op", "rest_conn", "rest_op"]
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=deps)
    assert err is not None and err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"


def test_rest_fetch_primitive_with_soap_connector_type_rejected():
    # A rest_fetch primitive forced to a SOAP connector_type is rejected.
    cfg = _sync_config(
        [_fetch_stage("s", connector_type="soap_client"), _send_stage("t", method="POST")],
        [{"from_stage": "s", "to_stage": "t"}],
    )
    deps = ["rest_src_conn", "rest_src_op", "rest_conn", "rest_op"]
    err = SyncPipelineBuilder.validate_config(cfg, depends_on=deps)
    assert err is not None and err.error_code == "SYNC_PIPELINE_CONFIG_INVALID"


def test_existing_rest_pipeline_unchanged_regression():
    # The REST-only pipeline still lowers to REST (no SOAP leakage).
    lowered = SyncPipelineBuilder.lower_config(_linear_with_map())
    assert lowered["target"]["connector_type"] == "rest"
    assert lowered["source"]["connector_type"] == "database"
