"""#184: the public symbol builder carries the profile facts the stream-profile proof reads.

Before #184 `recipes.materialization.build_symbol_table` set a profile ref on exactly
one kind of symbol, a listener operation's inbound request profile. A map, a REST or
database operation and a document cache reached the compiler with no profile at all.
The stream-profile proof would therefore have refused every map on the public route:
nothing states what the map reads or emits.

The field names are the ones each builder consumes, read from the builders rather
than invented here:
- `map_builder`: `source_profile_id` / `target_profile_id`;
- `connector_builder`, REST and SOAP: `request_profile_id` / `response_profile_id`;
- `connector_builder`, database Get: `read_profile_id`; database Send: `write_profile_id`;
- `document_cache_builder`: `profile_id`.

Only a `$ref:` naming a component of the same plan is carried. A literal id cannot be
classified offline, and #140 refuses a declared profile ref that is not a profile. A
reused component's requested config is not what the account stores. So both
contribute nothing, and a proof that needs the fact fails closed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError  # noqa: E402
from boomi_mcp.compiler.process_ir.pipeline import parse_and_compile_process_ir_v1  # noqa: E402
from boomi_mcp.errors import PROCESS_IR_SEMANTIC_PROFILE_MISMATCH  # noqa: E402
from boomi_mcp.models.integration_models import IntegrationComponentSpec  # noqa: E402
from boomi_mcp.recipes.materialization import build_symbol_table  # noqa: E402


def _component(key, component_type, **config):
    return IntegrationComponentSpec(key=key, type=component_type, config=config)


def _symbols(components, metadata=None):
    table = build_symbol_table(components, connector_metadata=metadata or {})
    return {symbol.ref: symbol for symbol in table.symbols}


_PROFILES = [
    _component("prof_read", "profile.json"),
    _component("prof_request", "profile.json"),
]


def test_a_created_map_carries_its_source_and_target_profiles():
    by_ref = _symbols(_PROFILES + [_component(
        "map", "transform.map", source_profile_id="$ref:prof_read", target_profile_id="$ref:prof_request")])
    assert by_ref["$ref:map"].input_profile_ref == "$ref:prof_read"
    assert by_ref["$ref:map"].output_profile_ref == "$ref:prof_request"


@pytest.mark.parametrize("config,expected_input,expected_output", [
    pytest.param({"request_profile_id": "$ref:prof_request", "response_profile_id": "$ref:prof_read"},
                 "$ref:prof_request", "$ref:prof_read", id="rest_request_and_response"),
    pytest.param({"read_profile_id": "$ref:prof_read"}, None, "$ref:prof_read", id="database_get"),
    pytest.param({"write_profile_id": "$ref:prof_request"}, "$ref:prof_request", None, id="database_send"),
])
def test_a_created_operation_carries_its_request_and_response_profiles(config, expected_input, expected_output):
    by_ref = _symbols(_PROFILES + [_component("op", "connector-action", **config)])
    assert (by_ref["$ref:op"].input_profile_ref, by_ref["$ref:op"].output_profile_ref) == (
        expected_input, expected_output)


def test_a_created_cache_carries_its_declared_profile():
    by_ref = _symbols(_PROFILES + [_component("cache", "documentcache", profile_id="$ref:prof_read")])
    assert by_ref["$ref:cache"].cache_profile_ref == "$ref:prof_read"


@pytest.mark.parametrize("component", [
    pytest.param(_component("map", "transform.map", source_profile_id="PROFILE-LITERAL-ID",
                            target_profile_id="$ref:prof_request"), id="literal_id"),
    pytest.param(_component("map", "transform.map", source_profile_id="$ref:not_in_plan",
                            target_profile_id="$ref:prof_request"), id="ref_outside_the_plan"),
])
def test_a_profile_the_plan_cannot_name_contributes_nothing(component):
    by_ref = _symbols(_PROFILES + [component])
    assert by_ref["$ref:map"].input_profile_ref is None


def test_a_reused_component_contributes_no_profile_facts():
    reused = _component("map", "transform.map", reference_only=True,
                        source_profile_id="$ref:prof_read", target_profile_id="$ref:prof_request")
    by_ref = _symbols(_PROFILES + [reused])
    assert (by_ref["$ref:map"].input_profile_ref, by_ref["$ref:map"].output_profile_ref) == (None, None)


def _plan(map_source, map_target):
    components = _PROFILES + [
        _component("conn", "connector-settings"),
        _component("get_op", "connector-action", connection_ref_key="conn",
                   response_profile_id="$ref:prof_read"),
        _component("patch_op", "connector-action", connection_ref_key="conn",
                   request_profile_id="$ref:prof_request"),
        _component("map", "transform.map", source_profile_id=map_source, target_profile_id=map_target),
    ]
    metadata = {"conn": ("rest", None), "get_op": ("rest", "GET"), "patch_op": ("rest", "PATCH")}
    return build_symbol_table(components, connector_metadata=metadata)


_DOC = {"version": "1", "body": {"kind": "sequence", "steps": [
    {"kind": "connector_call", "operation_ref": "$ref:get_op"},
    {"kind": "map_ref", "map_ref": "$ref:map"},
    {"kind": "connector_call", "operation_ref": "$ref:patch_op"},
    {"kind": "stop"}]}}


def test_a_public_plan_whose_map_matches_its_calls_compiles():
    parse_and_compile_process_ir_v1(_DOC, _plan("$ref:prof_read", "$ref:prof_request"))


def test_a_public_plan_whose_map_reads_the_wrong_profile_is_refused_at_the_map():
    with pytest.raises(ProcessIRCompileError) as excinfo:
        parse_and_compile_process_ir_v1(_DOC, _plan("$ref:prof_request", "$ref:prof_read"))
    assert (PROCESS_IR_SEMANTIC_PROFILE_MISMATCH, "/body/steps/1/map_ref") in [
        (item.code, item.path) for item in excinfo.value.diagnostics]
