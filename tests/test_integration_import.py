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
from boomi_mcp.models.authoring_workflow import parse_authoring_request_v1
from boomi_mcp.models.integration_models import IntegrationSpecV1  # noqa: F401
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
    # #159 S3: the draft re-validates as a canonical AuthoringRequestV1 with ONE
    # direct ProcessIR unit (REST -> REST has no executable recipe).
    request = parse_authoring_request_v1(r["authoring_request_draft"])
    assert request.intent.intent_kind == "process_ir"
    assert [unit.envelope.component_key for unit in request.intent.units] == ["main_process"]
    assert "integration_spec_draft" not in r
    assert r["selected_recipe_ref"] is None
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
    # DB source lacks binding/read material → honest blocking gaps naming the
    # canonical draft field, never a draft (#159 S3).
    assert r["ready_for_build"] is False
    assert "authoring_request_draft" not in r
    assert any(g["field"].startswith("authoring_request_draft.") for g in r["gaps"])
    assert r["selected_recipe_ref"]["recipe_id"] == "boomi.archetype.database_to_api_sync"


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
    # #159 S3: no retired composition tool is named; the step points at a
    # hand-authored canonical authoring request.
    assert not any("compose_archetypes" in step for step in r["next_steps"])
    assert any("AuthoringRequestV1" in step for step in r["next_steps"])


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
    assert summary["authoring_request_draft"] == generic["authoring_request_draft"]
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
    assert "authoring_request_draft" not in r


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
    assert "authoring_request_draft" not in r


def test_unsupported_transform_gap_keeps_map_stage():
    artifact = _rest_to_rest_artifact()
    artifact["transforms"] = [{"kind": "xslt", "stylesheet": "<xsl:stylesheet/>"}]
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM in _gap_codes(r)
    assert r["ready_for_build"] is False
    assert "authoring_request_draft" not in r
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
    assert "authoring_request_draft" not in r


def test_profile_uuid_without_index_gap():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["schema"] = {
        "profile_component_id": "12345678-1234-1234-1234-123456789012"
    }
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED in _gap_codes(r)
    assert r["ready_for_build"] is False
    assert "authoring_request_draft" not in r


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
    assert "authoring_request_draft" not in r


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
    assert "authoring_request_draft" not in r
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
    # silently dropped while ready_for_build stays true. #159 S3: the canonical
    # REST material carries NO request headers, so declared headers are a
    # blocking unsupported-construct gap per side (never dropped); the literal
    # query value still reaches the derived parameters.
    artifact = _rest_to_rest_artifact()
    artifact["source"]["request_headers"] = {"Accept": "application/json"}
    artifact["target"]["query_parameters"] = {"mode": "upsert"}
    artifact["target"]["request_headers"] = {"X-Api-Version": "2"}
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is False
    assert "authoring_request_draft" not in r
    fetch = r["preset_parameters"]["source"]["fetch_request"]
    send = r["preset_parameters"]["target"]["send_request"]
    assert fetch["request_headers"] == {"Accept": "application/json"}
    assert send["query_parameters"] == {"mode": "upsert"}
    assert send["request_headers"] == {"X-Api-Version": "2"}
    assert {(g["code"], g["field"]) for g in r["gaps"]} >= {
        (MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, "source.request_headers"),
        (MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, "target.request_headers"),
    }
    # Without the headers the same draft is ready and carries the query value.
    del artifact["source"]["request_headers"], artifact["target"]["request_headers"]
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is True, r["gaps"]
    assert "upsert" in json.dumps(r["authoring_request_draft"])


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
    assert "authoring_request_draft" not in r
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
    # #159 S3: non-secret metadata still propagates into the derived
    # parameters; the canonical draft gaps the headers it cannot carry.
    artifact = _rest_to_rest_artifact()
    artifact["source"]["request_headers"] = {"Accept": "application/json"}
    artifact["target"]["query_parameters"] = {"mode": "upsert"}
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is False
    assert [(g["code"], g["field"]) for g in r["gaps"]] == [
        (MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, "source.request_headers"),
    ]
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


def test_declared_retry_dlq_schedule_block_not_silently_dropped():
    # Architect review: reliability/schedule the preset can't emit must surface
    # as blocking unsupported-construct gaps, never as build-ready confirmed
    # facts that were never honored.
    artifact = _rest_to_rest_artifact()
    artifact["retry"] = {"max_attempts": 5}
    artifact["error_handling"] = {"dlq": "route_to_queue"}
    artifact["trigger"] = {"kind": "scheduled"}
    r = act("generic_integration_description", artifact)
    fields = {
        (g["code"], g["field"])
        for g in r["gaps"]
        if g["code"] == MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT
    }
    assert (MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, "retry") in fields
    assert (MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, "error_handling") in fields
    assert (MIGRATION_IMPORT_UNSUPPORTED_CONSTRUCT, "trigger") in fields
    assert r["ready_for_build"] is False
    assert "authoring_request_draft" not in r
    # Never recorded as a confirmed fact implying it was honored.
    assert not any("retry" in f["source"] for f in r["confirmed_facts"])
    assert not any(f["source"] == "artifact:trigger" for f in r["confirmed_facts"])


def test_deployment_intent_is_assumption_not_blocking_gap():
    # Deployment is a separate orchestrate_deploy lifecycle step, not part of
    # the emitted IntegrationSpecV1 draft — it must NOT block build-readiness.
    artifact = _rest_to_rest_artifact()
    artifact["deployment"] = {"atom": "local-runtime", "environment": "dev"}
    r = act("generic_integration_description", artifact)
    assert r["ready_for_build"] is True
    assert not any(
        g["field"] == "deployment" for g in r["gaps"]
    )
    assert any(
        "deployment" in a["source"] for a in r["inferred_assumptions"]
    )


def test_singleton_transform_dict_not_silently_dropped():
    # Architect review: a transform expressed as a single object (not a list)
    # must still be validated — an unsupported one blocks.
    artifact = _rest_to_rest_artifact()
    artifact["transforms"] = {"kind": "xslt", "stylesheet": "<xsl/>"}
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM in _gap_codes(r)
    assert r["ready_for_build"] is False
    # A supported singleton transform is accepted like a one-element list.
    artifact["transforms"] = {
        "kind": "field_mapping",
        "mappings": [{"from": "id", "to": "customer_id"}],
    }
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_UNSUPPORTED_TRANSFORM not in _gap_codes(r)


def test_transforms_wrong_type_gapped():
    artifact = _rest_to_rest_artifact()
    artifact["transforms"] = "xslt"
    r = act("generic_integration_description", artifact)
    gap = next(
        g for g in r["gaps"] if g["field"] == "transforms"
    )
    assert gap["code"] == MIGRATION_IMPORT_INVALID_INPUT


def test_singleton_transform_keeps_map_stage_like_list_form():
    # Repo Codex review: the singleton-object form must not hide transform
    # intent — the pipeline draft's map stage matches the list form exactly.
    artifact = _rest_to_rest_artifact()
    del artifact["mappings"]
    artifact["transforms"] = {"kind": "xslt", "stylesheet": "<xsl/>"}
    singleton_stages = [
        s["kind"]
        for s in act("generic_integration_description", artifact)["pipeline_draft"][
            "stages"
        ]
    ]
    artifact["transforms"] = [{"kind": "xslt", "stylesheet": "<xsl/>"}]
    list_stages = [
        s["kind"]
        for s in act("generic_integration_description", artifact)["pipeline_draft"][
            "stages"
        ]
    ]
    assert singleton_stages == list_stages == ["fetch", "map", "send"]
    # A flow with no transform at all correctly omits the map stage.
    artifact2 = _rest_to_rest_artifact()
    del artifact2["mappings"]
    no_xform = [
        s["kind"]
        for s in act("generic_integration_description", artifact2)["pipeline_draft"][
            "stages"
        ]
    ]
    assert no_xform == ["fetch", "send"]


def test_by_reference_index_unresolved_mapping_gapped():
    # Architect review: a #95-indexed existing profile has no inline profile for
    # the archetype to validate against, so an unindexed mapping path/leaf must
    # gap here (never pass an invented key into the draft).
    artifact = _rest_to_rest_artifact()
    artifact["source"]["schema"] = {
        "profile_component_id": "12345678-1234-1234-1234-123456789012",
        "field_index": {
            "produced_by": "index_profile_component",
            "mappable_paths": ["Root/id"],
        },
    }
    # Full path not in the index → PROFILE_INDEX_REQUIRED, not passed through.
    artifact["mappings"] = [{"from": "Root/not_indexed", "to": "contact_email"}]
    r = act("generic_integration_description", artifact)
    assert (MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED, "mappings[0].from") in {
        (g["code"], g["field"]) for g in r["gaps"]
    }
    assert "not_indexed" not in json.dumps(r.get("preset_parameters") or {})
    # A bare leaf name not in the index also gaps (never invented).
    artifact["mappings"] = [{"from": "ghost", "to": "contact_email"}]
    r = act("generic_integration_description", artifact)
    assert MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED in _gap_codes(r)
    # An indexed path resolves cleanly.
    artifact["mappings"] = [{"from": "Root/id", "to": "contact_email"}]
    r = act("generic_integration_description", artifact)
    assert not any(
        g["code"] == MIGRATION_IMPORT_PROFILE_INDEX_REQUIRED for g in r["gaps"]
    )


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
    # #159 S3: the canonical draft's validation gaps name the draft field.
    param_gaps = [g for g in r["gaps"] if g["field"].startswith("authoring_request_draft.")]
    assert param_gaps, "expected per-field validation gaps for the bare DB source"
    for gap in param_gaps:
        assert "field_errors" not in (gap.get("hint") or "")


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


# ---------------------------------------------------------------------------
# #159 S3: the canonical draft reaches plan and compile
# ---------------------------------------------------------------------------


def _listener_to_rest_artifact():
    return {
        "name": "Order Hook",
        "source": {"protocol": "webhook", "object_name": "orders",
                   "schema": {"profile": _SOURCE_PROFILE}},
        "target": {"protocol": "rest", "base_url": "https://t.example.com", "path": "/x",
                   "method": "POST", "auth": {"mode": "none"},
                   "schema": {"profile": _TARGET_PROFILE}},
        "trigger": {"kind": "listener"},
        "mappings": [{"from": "id", "to": "customer_id"}, {"from": "email", "to": "contact_email"}],
    }


def _reused_connections_artifact():
    artifact = _rest_to_rest_artifact()
    artifact["source"]["connection"] = {"component_id": "11111111-1111-4111-8111-111111111111"}
    artifact["target"]["connection"] = {"component_name": "Existing Target REST"}
    artifact["target"]["query_parameters"] = {"mode": "upsert"}
    return artifact


@pytest.mark.parametrize("factory, preset", [
    (_rest_to_rest_artifact, "api_to_api_sync"),
    (_listener_to_rest_artifact, "http_listener_to_rest"),
])
def test_a_ready_draft_reaches_plan_and_compile(factory, preset):
    """Every shape whose draft was build-ready before the cutover (REST -> REST via
    the retired API->API archetype; listener -> REST) is ready now, and its served
    draft, passed VERBATIM, plans and compiles through ``build_integration`` with
    only the account listing stubbed. (A draft that REUSES connections is planned
    below; its compile reads the reused connection's stored identity from the
    account, which an offline stub cannot answer.)"""
    from unittest.mock import MagicMock, patch

    from boomi_mcp.categories import integration_builder as ib

    r = act("generic_integration_description", factory())
    assert r["selected_preset"] == preset
    assert r["ready_for_build"] is True, r["gaps"]
    draft = r["authoring_request_draft"]
    with patch.object(ib, "paginate_metadata", return_value=[]):
        planned = ib.build_integration_action(
            MagicMock(), "p", "plan", config={"authoring_request": draft})
        assert planned["_success"] is True, planned
        compiled = ib.build_integration_action(
            MagicMock(), "p", "compile", config={"authoring_request": draft})
    assert compiled["_success"] is True, compiled
    assert compiled["authoring_result"]["revision_binding"]["compile_hash"]


def test_a_ready_draft_reusing_connections_plans():
    from unittest.mock import MagicMock, patch

    from boomi_mcp.categories import integration_builder as ib

    r = act("generic_integration_description", _reused_connections_artifact())
    assert r["ready_for_build"] is True, r["gaps"]
    rows = {c["key"]: c for c in r["authoring_request_draft"]["intent"]["components"]}
    assert "source_rest_source_connection" in rows and "target_rest_connection" in rows
    with patch.object(ib, "paginate_metadata", return_value=[]):
        planned = ib.build_integration_action(
            MagicMock(), "p", "plan", config={"authoring_request": r["authoring_request_draft"]})
    assert planned["_success"] is True, planned


def test_every_pinned_preset_recipe_is_a_production_executable():
    """The table the draft pins from names only production executables, so a
    registration retirement cannot orphan a served draft."""
    from boomi_mcp.categories.integration_import import _PRESET_RECIPES, _selected_recipe_ref
    from boomi_mcp.recipes import production_registry

    registry = production_registry()
    for preset, (recipe_id, version) in _PRESET_RECIPES.items():
        descriptor = registry.resolve(recipe_id, version)
        assert descriptor.entry_kind == "executable_recipe", preset
        assert _selected_recipe_ref(preset) == {
            "recipe_id": recipe_id, "recipe_version": version, "entry_kind": "executable_recipe"}
