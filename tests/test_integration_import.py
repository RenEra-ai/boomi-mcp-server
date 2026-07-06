"""Issue #48 (M7.2): pure action tests for ``import_integration_draft_action``.

Covers the migration-import contract: happy REST→REST and DB→REST drafts,
provenance isolation for source-tool exports (anti-template), the six
MIGRATION_IMPORT_* gap/error codes, #95 profile-index enforcement,
confirmed-facts vs inferred-assumptions separation, anti-template registry
proofs, and determinism. Runs with PYTHONPATH=src; no Boomi/credential access.
"""

import json

import pytest

from boomi_mcp.categories.integration_import import (
    MIGRATION_IMPORT_AMBIGUOUS_MAPPING,
    MIGRATION_IMPORT_INVALID_INPUT,
    MIGRATION_IMPORT_MISSING_CREDENTIAL,
    MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED,
    MIGRATION_IMPORT_UNKNOWN_PROTOCOL,
    MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT,
    MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM,
    import_integration_draft_action as act,
)
from boomi_mcp.models.integration_models import IntegrationSpecV1
from boomi_mcp.patterns import PatternKind, PatternRegistry


# ---------------------------------------------------------------------------
# Fixtures — structured artifacts (NOT product templates)
# ---------------------------------------------------------------------------


def _json_profile(children):
    return {
        "format": "json",
        "root": {"name": "Root", "kind": "object", "children": children},
    }


_SOURCE_PROFILE = _json_profile(
    [
        {"name": "id", "kind": "simple", "data_type": "number", "required": True},
        {"name": "email", "kind": "simple", "data_type": "character", "required": True},
    ]
)

_TARGET_PROFILE = _json_profile(
    [
        {"name": "customer_id", "kind": "simple", "data_type": "number", "required": True},
        {"name": "contact_email", "kind": "simple", "data_type": "character", "required": True},
    ]
)


def _rest_to_rest_artifact():
    return {
        "name": "CRM Contact Sync",
        "source": {
            "protocol": "rest",
            "base_url": "https://source.example.com",
            "path": "/v1/contacts",
            "auth": {"mode": "none"},
            "schema": {"profile": _SOURCE_PROFILE},
        },
        "target": {
            "protocol": "rest",
            "base_url": "https://target.example.com",
            "path": "/v1/customers",
            "method": "POST",
            "auth": {"mode": "none"},
            "schema": {"profile": _TARGET_PROFILE},
        },
        "trigger": {"kind": "manual"},
        "mappings": [
            {"from": "id", "to": "customer_id"},
            {"from": "email", "to": "contact_email"},
        ],
    }


def _gap_codes(response):
    return [g["code"] for g in response["gaps"]]


def _assert_flags(env):
    assert env["read_only"] is True
    assert env["boomi_mutation"] is False
    assert env["raw_xml_exposed"] is False


# ---------------------------------------------------------------------------
# Happy paths — generic description fixture
# ---------------------------------------------------------------------------


def test_rest_to_rest_full_draft():
    r = act("generic_integration_description", _rest_to_rest_artifact())
    assert r["_success"] is True
    _assert_flags(r)
    assert r["ready_for_build"] is True
    assert r["gaps"] == []
    assert r["selected_preset"] == "api_to_api_sync"
    stages = [s["kind"] for s in r["pipeline_draft"]["stages"]]
    assert stages == ["fetch", "map", "send"]
    assert r["preset_parameters"]["naming"]["integration_name"] == "CRM Contact Sync"
    # The draft re-validates as a real IntegrationSpecV1 — never a broken input.
    IntegrationSpecV1.model_validate(r["integration_spec_draft"])
    assert (
        "build_integration(action='plan'" in r["next_steps"][0]
    ), "build-ready draft must point at the normal build workflow"


def test_rest_to_rest_artifact_as_json_string():
    r = act("generic_integration_description", json.dumps(_rest_to_rest_artifact()))
    assert r["_success"] is True and r["ready_for_build"] is True


def test_db_to_rest_selects_database_preset():
    r = act(
        "generic_integration_description",
        {
            "name": "Orders Export",
            "source": {"protocol": "database"},
            "target": {
                "protocol": "rest",
                "base_url": "https://t.example.com",
                "path": "/orders",
                "schema": {"profile": _TARGET_PROFILE},
            },
            "mappings": [{"from": "ORDER_ID", "to": "customer_id"}],
        },
    )
    assert r["_success"] is True
    assert r["selected_preset"] == "database_to_api_sync"
    assert [s["kind"] for s in r["pipeline_draft"]["stages"]] == ["read", "map", "send"]
    # DB source lacks binding/read_operation → honest per-field blocking gaps,
    # never a spec draft.
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r
    assert any(g["field"].startswith("preset_parameters.") for g in r["gaps"])


def test_listener_flow_selects_listener_preset():
    r = act(
        "generic_integration_description",
        {
            "source": {"protocol": "webhook"},
            "target": {"protocol": "rest", "base_url": "https://t.example.com", "path": "/x"},
            "trigger": {"kind": "listener"},
        },
    )
    assert r["selected_preset"] == "http_listener_to_rest"
    assert [s["kind"] for s in r["pipeline_draft"]["stages"]] == ["listener", "send"]


def test_no_matching_preset_points_at_compose():
    r = act(
        "generic_integration_description",
        {"source": {"protocol": "database"}, "target": {"protocol": "database"}},
    )
    assert r["_success"] is True
    assert r["selected_preset"] is None
    assert r["preset_parameters"] is None
    assert r["pipeline_draft"] is None
    assert r["ready_for_build"] is False
    assert any("compose_archetypes" in step for step in r["next_steps"])


# ---------------------------------------------------------------------------
# Source-tool export summary — provenance isolation (anti-template proof #1)
# ---------------------------------------------------------------------------


def test_export_summary_same_draft_as_generic_and_provenance_only():
    generic = act("generic_integration_description", _rest_to_rest_artifact())
    summary = act(
        "source_tool_export_summary",
        {
            "product": "LegacyESB",
            "version": "9.1",
            "tool": "esb-exporter",
            "flow": _rest_to_rest_artifact(),
        },
    )
    assert summary["_success"] is True
    assert summary["input_provenance"]["product"] == "LegacyESB"
    assert summary["input_provenance"]["version"] == "9.1"
    # Product identity must not steer the semantic output: same preset, same
    # pipeline, same parameters, same spec draft as the generic description.
    assert summary["selected_preset"] == generic["selected_preset"]
    assert summary["pipeline_draft"] == generic["pipeline_draft"]
    assert summary["preset_parameters"] == generic["preset_parameters"]
    assert summary["integration_spec_draft"] == generic["integration_spec_draft"]
    # ... and the product/version tokens appear ONLY under input_provenance.
    scrubbed = {k: v for k, v in summary.items() if k != "input_provenance"}
    assert "LegacyESB" not in json.dumps(scrubbed)


def test_export_summary_flat_keys_without_flow_wrapper():
    artifact = dict(_rest_to_rest_artifact(), product="OtherTool")
    r = act("source_tool_export_summary", artifact)
    assert r["_success"] is True
    assert r["selected_preset"] == "api_to_api_sync"
    assert r["input_provenance"]["product"] == "OtherTool"


# ---------------------------------------------------------------------------
# Negative analyses — structured gaps, _success stays True, draft suppressed
# ---------------------------------------------------------------------------


def test_missing_credential_gap():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["auth"] = {"mode": "basic"}
    r = act("generic_integration_description", artifact)
    assert r["_success"] is True
    assert MIGRATION_IMPORT_MISSING_CREDENTIAL in _gap_codes(r)
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r


def test_plaintext_secret_in_auth_is_gapped_and_never_forwarded():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["auth"] = {"mode": "basic", "password": "hunter2"}
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_MISSING_CREDENTIAL in _gap_codes(r)
    assert "hunter2" not in json.dumps(r)


def test_unknown_protocol_gap():
    r = act(
        "generic_integration_description",
        {
            "source": {"protocol": "kafka"},
            "target": {"protocol": "rest", "base_url": "https://t.example.com"},
        },
    )
    assert MIGRATION_IMPORT_UNKNOWN_PROTOCOL in _gap_codes(r)
    assert r["selected_preset"] is None
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r


def test_unsupported_transform_gap_keeps_map_stage():
    artifact = _rest_to_rest_artifact()
    artifact["transforms"] = [{"kind": "xslt", "stylesheet": "<xsl:stylesheet/>"}]
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM in _gap_codes(r)
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r
    # The draft still shows a transform happens; its specifics live in gaps[].
    assert "map" in [s["kind"] for s in r["pipeline_draft"]["stages"]]


def test_script_transform_is_unsupported():
    artifact = _rest_to_rest_artifact()
    artifact["transforms"] = [{"kind": "script", "body": "return doc"}]
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM in _gap_codes(r)
    assert "return doc" not in json.dumps(r["gaps"])


def test_ambiguous_mapping_gap_lists_candidates():
    source_profile = _json_profile(
        [
            {
                "name": "a",
                "kind": "object",
                "children": [
                    {"name": "id", "kind": "simple", "data_type": "number"}
                ],
            },
            {
                "name": "b",
                "kind": "object",
                "children": [
                    {"name": "id", "kind": "simple", "data_type": "number"}
                ],
            },
        ]
    )
    artifact = _rest_to_rest_artifact()
    artifact["source"]["schema"] = {"profile": source_profile}
    artifact["mappings"] = [{"from": "id", "to": "customer_id"}]
    r = act("generic_integration_description", artifact)
    gaps = [g for g in r["gaps"] if g["code"] == MIGRATION_IMPORT_AMBIGUOUS_MAPPING]
    assert len(gaps) == 1
    assert gaps[0]["details"]["candidates"] == ["Root/a/id", "Root/b/id"]
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r


def test_profile_uuid_without_index_gap():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["schema"] = {
        "profile_component_id": "12345678-1234-1234-1234-123456789012"
    }
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED in _gap_codes(r)
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r


def test_profile_uuid_with_95_index_accepted():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["schema"] = {
        "profile_component_id": "12345678-1234-1234-1234-123456789012",
        "field_index": {
            "produced_by": "index_profile_component",
            "mappable_paths": ["Root/id", "Root/email"],
        },
    }
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED not in _gap_codes(r)
    # By-reference schemas resolve mappings but are not inline build inputs.
    assert r["ready_for_build"] is False
    assert any("field index" in s or "#95" in s for s in r["next_steps"])


def test_mapping_referencing_uuid_directly_requires_index():
    artifact = _rest_to_rest_artifact()
    artifact["mappings"] = [
        {"from": "12345678-1234-1234-1234-123456789012", "to": "customer_id"}
    ]
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED in _gap_codes(r)


def test_inferred_schema_confirmation_required_blocks_build():
    artifact = _rest_to_rest_artifact()
    # Null-valued sample → ambiguous type → upstream ready_for_builder=False.
    artifact["source"]["schema"] = {
        "infer": {
            "source_type": "profile_from_sample_json",
            "artifact": json.dumps({"id": None, "email": "a@b.c"}),
        }
    }
    r = act("generic_integration_description", artifact)
    assert r["_success"] is True
    assert "PROFILE_INFERENCE_CONFIRMATION_REQUIRED" in _gap_codes(r)
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r


def test_inferred_schema_clean_sample_is_build_ready():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["schema"] = {
        "infer": {
            "source_type": "profile_from_sample_json",
            "artifact": json.dumps({"id": 1, "email": "a@b.c"}),
        }
    }
    r = act("generic_integration_description", artifact)
    assert r["_success"] is True
    assert r["ready_for_build"] is True
    assert any(a["source"].startswith("inferred:") for a in r["inferred_assumptions"])


def test_non_get_rest_source_blocks_instead_of_silent_get():
    # Codex review r1: a POST/PUT source cannot be represented by the GET-only
    # rest_fetch primitive — it must block, never silently draft a GET.
    artifact = _rest_to_rest_artifact()
    artifact["source"]["method"] = "POST"
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNKNOWN_PROTOCOL in _gap_codes(r)
    gap = next(g for g in r["gaps"] if g["code"] == MIGRATION_IMPORT_UNKNOWN_PROTOCOL)
    assert gap["field"] == "source.method"
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r
    # Explicit GET stays build-ready.
    artifact["source"]["method"] = "GET"
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is True


def test_non_get_source_unrecognized_method_value_not_echoed():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["method"] = "s3cr3t-token-value"
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNKNOWN_PROTOCOL in _gap_codes(r)
    assert "s3cr3t-token-value" not in json.dumps(r["gaps"])


def test_target_query_and_headers_propagate_into_draft():
    # Codex review r1: declared request metadata must reach the draft, not be
    # silently dropped while ready_for_build stays true.
    artifact = _rest_to_rest_artifact()
    artifact["source"]["request_headers"] = {"Accept": "application/json"}
    artifact["target"]["query_parameters"] = {"mode": "upsert"}
    artifact["target"]["request_headers"] = {"X-Api-Version": "2"}
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is True
    fetch = r["preset_parameters"]["source"]["fetch_request"]
    send = r["preset_parameters"]["target"]["send_request"]
    assert fetch["request_headers"] == {"Accept": "application/json"}
    assert send["query_parameters"] == {"mode": "upsert"}
    assert send["request_headers"] == {"X-Api-Version": "2"}
    assert "upsert" in json.dumps(r["integration_spec_draft"])


def test_db_preset_target_query_params_use_literal_list_form():
    r = act(
        "generic_integration_description",
        {
            "name": "Orders Export",
            "source": {"protocol": "database"},
            "target": {
                "protocol": "rest",
                "base_url": "https://t.example.com",
                "path": "/orders",
                "query_parameters": {"mode": "bulk"},
                "schema": {"profile": _TARGET_PROFILE},
            },
        },
    )
    send = r["preset_parameters"]["target"]["send_request"]
    # The DB preset emits literal query params onto the operation (typed list).
    assert send["query_parameters"] == [
        {"name": "mode", "value_source": "literal", "literal_value": "bulk"}
    ]
    assert "request_headers" not in send


def test_db_preset_target_headers_block_not_silently_dropped():
    # Codex review r2: database_to_api_sync defers connection default_headers
    # (never emitted), so declared target headers must BLOCK, never be stashed
    # where they would be silently lost.
    r = act(
        "generic_integration_description",
        {
            "name": "Orders Export",
            "source": {"protocol": "database"},
            "target": {
                "protocol": "rest",
                "base_url": "https://t.example.com",
                "path": "/orders",
                "request_headers": {"X-Api-Version": "2"},
                "schema": {"profile": _TARGET_PROFILE},
            },
        },
    )
    assert MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT in _gap_codes(r)
    gap = next(
        g for g in r["gaps"] if g["code"] == MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT
    )
    assert gap["field"] == "target.request_headers"
    assert gap["severity"] == "blocking"
    # Never stashed in default_headers, never echoed.
    binding = r["preset_parameters"]["target"]["binding"]
    assert "default_headers" not in binding.get("settings", {})
    assert r["ready_for_build"] is False


def test_secret_shaped_header_rejected_not_echoed():
    # Codex review r2: a credential smuggled through request_headers must be
    # gapped, never copied into the returned draft.
    artifact = _rest_to_rest_artifact()
    artifact["target"]["request_headers"] = {"Authorization": "Bearer sk-live-xyz"}
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_MISSING_CREDENTIAL in _gap_codes(r)
    gap = next(
        g
        for g in r["gaps"]
        if g["code"] == MIGRATION_IMPORT_MISSING_CREDENTIAL
        and g["field"] == "target.request_headers"
    )
    assert gap["severity"] == "blocking"
    assert "sk-live-xyz" not in json.dumps(r)
    assert "Bearer" not in json.dumps(r)
    assert r["ready_for_build"] is False
    assert "integration_spec_draft" not in r
    # The safe payload_profile still made it through; only the header is dropped.
    send = r["preset_parameters"]["target"]["send_request"]
    assert "request_headers" not in send


def test_secret_shaped_query_param_rejected_not_echoed():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["query_parameters"] = {"api_key": "sk-secret-123"}
    r = act("generic_integration_description", artifact)
    codes_fields = [
        (g["code"], g["field"]) for g in r["gaps"]
    ]
    assert (MIGRATION_IMPORT_MISSING_CREDENTIAL, "source.query_parameters") in codes_fields
    assert "sk-secret-123" not in json.dumps(r)
    fetch = r["preset_parameters"]["source"].get("fetch_request", {})
    assert "query_parameters" not in fetch


def test_non_secret_headers_and_query_still_propagate():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["request_headers"] = {"Accept": "application/json"}
    artifact["target"]["query_parameters"] = {"mode": "upsert"}
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is True
    assert (
        r["preset_parameters"]["source"]["fetch_request"]["request_headers"]
        == {"Accept": "application/json"}
    )
    assert (
        r["preset_parameters"]["target"]["send_request"]["query_parameters"]
        == {"mode": "upsert"}
    )


def test_rejected_vocabulary_values_never_echoed_in_gaps():
    # Codex review r1: values that FAILED vocabulary validation are arbitrary
    # caller content — a secret misplaced there must not leak through gaps.
    r = act(
        "generic_integration_description",
        {
            "source": {"protocol": "hunter2-secret-protocol"},
            "target": {"protocol": "rest", "base_url": "https://t.example.com"},
            "transforms": [{"kind": "hunter2-secret-kind"}],
        },
    )
    dumped = json.dumps(r["gaps"])
    assert "hunter2" not in dumped
    codes = _gap_codes(r)
    assert MIGRATION_IMPORT_UNKNOWN_PROTOCOL in codes
    assert MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM in codes


def test_unrecognized_auth_mode_value_never_echoed():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["auth"] = {"mode": "hunter2-mode"}
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_MISSING_CREDENTIAL in _gap_codes(r)
    assert "hunter2" not in json.dumps(r)


# ---------------------------------------------------------------------------
# Facts vs assumptions separation
# ---------------------------------------------------------------------------


def test_confirmed_facts_and_assumptions_are_separated():
    artifact = _rest_to_rest_artifact()
    del artifact["name"]  # force a defaulted value → assumption
    del artifact["trigger"]
    r = act("generic_integration_description", artifact)
    fact_sources = {f["source"] for f in r["confirmed_facts"]}
    assumption_sources = {a["source"] for a in r["inferred_assumptions"]}
    assert all(s.startswith(("artifact:", "options:")) for s in fact_sources)
    assert all(s.startswith("inferred:") for s in assumption_sources)
    assert any("source protocol" in f["statement"] for f in r["confirmed_facts"])
    assert any(
        "Imported Integration" in a["statement"] for a in r["inferred_assumptions"]
    )
    assert any("manual" in a["statement"] for a in r["inferred_assumptions"])


# ---------------------------------------------------------------------------
# Malformed tool input — the ONLY _success=False path
# ---------------------------------------------------------------------------


def test_free_text_artifact_rejected():
    r = act("generic_integration_description", "sync my CRM to the billing API please")
    assert r["_success"] is False
    assert r["code"] == MIGRATION_IMPORT_INVALID_INPUT
    assert r["error_code"] == MIGRATION_IMPORT_INVALID_INPUT
    _assert_flags(r)
    assert "sync my CRM" not in json.dumps(r)  # content never echoed


def test_non_object_artifact_rejected():
    for bad in ([1, 2], 42, json.dumps([1, 2])):
        r = act("generic_integration_description", bad)
        assert r["_success"] is False
        assert r["code"] == MIGRATION_IMPORT_INVALID_INPUT


def test_unknown_source_type_rejected():
    r = act("legacy_vendor_export", {})
    assert r["_success"] is False
    assert r["code"] == MIGRATION_IMPORT_INVALID_INPUT
    assert r["details"]["supported_source_types"] == [
        "generic_integration_description",
        "source_tool_export_summary",
    ]
    _assert_flags(r)


def test_bad_options_json_rejected():
    r = act("generic_integration_description", {}, options="{bad json")
    assert r["_success"] is False
    assert r["code"] == MIGRATION_IMPORT_INVALID_INPUT
    assert r["field"] == "options"
    _assert_flags(r)


def test_options_non_object_rejected():
    r = act("generic_integration_description", {}, options="[1,2]")
    assert r["_success"] is False and r["code"] == MIGRATION_IMPORT_INVALID_INPUT


def test_options_override_naming():
    r = act(
        "generic_integration_description",
        _rest_to_rest_artifact(),
        options={"component_prefix": "MIGR"},
    )
    assert r["preset_parameters"]["naming"]["component_prefix"] == "MIGR"
    # QA bug #151: options-supplied values carry honest 'options:' provenance.
    assert any(
        f["source"] == "options:component_prefix" for f in r["confirmed_facts"]
    )


def test_options_integration_name_has_options_provenance():
    artifact = _rest_to_rest_artifact()
    del artifact["name"]
    r = act(
        "generic_integration_description",
        artifact,
        options={"integration_name": "From Options"},
    )
    assert r["preset_parameters"]["naming"]["integration_name"] == "From Options"
    assert any(
        f["source"] == "options:integration_name" for f in r["confirmed_facts"]
    )
    # Never misattributed to a nonexistent artifact path.
    assert not any(f["source"] == "artifact:name" for f in r["confirmed_facts"])


def test_artifact_name_takes_precedence_over_options():
    r = act(
        "generic_integration_description",
        _rest_to_rest_artifact(),
        options={"integration_name": "Ignored"},
    )
    assert r["preset_parameters"]["naming"]["integration_name"] == "CRM Contact Sync"


def test_param_validation_gap_hints_do_not_dangle():
    # QA bug #150: per-field build-validation gaps must not point at a
    # field_errors[] key that this tool's response does not carry.
    r = act(
        "generic_integration_description",
        {
            "name": "Orders Export",
            "source": {"protocol": "database"},
            "target": {
                "protocol": "rest",
                "base_url": "https://t.example.com",
                "path": "/orders",
                "schema": {"profile": _TARGET_PROFILE},
            },
        },
    )
    param_gaps = [g for g in r["gaps"] if g["code"] == "PARAM_VALIDATION_FAILED"]
    assert param_gaps, "expected per-field validation gaps for the bare DB source"
    for gap in param_gaps:
        assert "field_errors" not in gap.get("hint", "")


# ---------------------------------------------------------------------------
# Anti-template proofs (registry unchanged, presets always pre-existing)
# ---------------------------------------------------------------------------

_EXPECTED_ARCHETYPES = {
    "stub_minimal_integration",
    "database_to_api_sync",
    "api_to_api_sync",
    "api_to_database_sync",
    "http_listener_to_rest",
    "http_listener_to_db",
}


def test_selected_presets_are_existing_registry_archetypes():
    registry = PatternRegistry.from_package("boomi_mcp.patterns")
    names = {
        p.metadata.name
        for p in registry.list_patterns(kind=PatternKind.ARCHETYPE)
    }
    # Importing the new module must not have registered any new/product-named
    # archetype (no preset forks).
    assert names == _EXPECTED_ARCHETYPES
    r = act("generic_integration_description", _rest_to_rest_artifact())
    assert r["selected_preset"] in names


def test_no_product_specific_branching():
    """Two export summaries differing ONLY in product produce identical drafts."""
    base = {"flow": _rest_to_rest_artifact()}
    r1 = act("source_tool_export_summary", dict(base, product="VendorA"))
    r2 = act("source_tool_export_summary", dict(base, product="VendorB"))
    for key in ("selected_preset", "pipeline_draft", "preset_parameters", "gaps"):
        assert r1[key] == r2[key]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "artifact_factory",
    [
        _rest_to_rest_artifact,
        lambda: {
            "source": {"protocol": "sftp"},
            "target": {"protocol": "rest", "base_url": "https://t.example.com"},
        },
    ],
)
def test_repeat_calls_identical(artifact_factory):
    a = act("generic_integration_description", artifact_factory())
    b = act("generic_integration_description", artifact_factory())
    assert a == b
