"""Tests for the curated operational-gotcha catalog (issue #77, M9.1).

Pure-unit against boomi_mcp — no server import, no SDK calls, no ML deps. Covers
the catalog/entry schema + import-time validation, lexical ranking, the
deterministic issue_ids exact lookup, the empty_query / no_match envelopes,
provenance + verification fields, and the anti-template lint over served prose.
"""

import re
import sys
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.kb.operational_gotchas import (  # noqa: E402
    CATEGORIES,
    DETECTIONS,
    FREQUENCIES,
    GOTCHA_ENTRY_SCHEMA,
    OPERATIONAL_GOTCHA_ENTRIES,
    OPERATIONAL_GOTCHA_ENTRY_COUNT,
    OPERATIONAL_GOTCHA_REQUIRED_FIELDS,
    VERIFICATION_STATUSES,
    get_operational_gotchas_catalog,
    list_operational_gotchas_index,
    render_operational_gotchas_resource,
    search_operational_gotchas,
    triage_symptoms,
    valid_operational_gotcha_ids,
)

# The 2026-06-14 architect-course comment's four ★ high-value entries.
STARRED_IDS = {
    "tracked_field_repeating_first_occurrence",
    "cdc_partial_dataset_mass_delete",
    "return_documents_deferred_batch",
    "test_mode_extensions_shared_across_users",
}

# The six issue-#77 body domains; the seed must represent every one.
EXPECTED_DOMAINS = {
    "listener_wss",
    "platform_entities",
    "connector_behavior",
    "deployment_testing",
    "process_serialization",
    "marketplace",
}

_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_RAW_XML_RE = re.compile(r"<\?xml|</?[a-zA-Z][\w:-]*>")
_SQL_CRUD_RE = re.compile(
    r"(?i)\b(select\s+.+\s+from|insert\s+into|update\s+\w+\s+set|delete\s+from)\b"
)


def _flatten_strings(node):
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _flatten_strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _flatten_strings(value)


def _served_prose_blob():
    catalog = get_operational_gotchas_catalog()
    return "\n".join(_flatten_strings(catalog)) + "\n" + render_operational_gotchas_resource()


# ---------------------------------------------------------------------------
# Catalog + entry schema + import-time validation
# ---------------------------------------------------------------------------


def test_catalog_count_matches_entries():
    catalog = get_operational_gotchas_catalog()
    assert catalog["entry_count"] == OPERATIONAL_GOTCHA_ENTRY_COUNT
    assert catalog["entry_count"] == len(catalog["entries"])
    assert catalog["read_only"] is True
    assert catalog["entry_schema"] is not GOTCHA_ENTRY_SCHEMA  # deepcopied
    assert catalog["entry_schema"]["required"] == list(OPERATIONAL_GOTCHA_REQUIRED_FIELDS)


def test_every_entry_has_required_fields_and_valid_vocabularies():
    for gid, entry in OPERATIONAL_GOTCHA_ENTRIES.items():
        for field in OPERATIONAL_GOTCHA_REQUIRED_FIELDS:
            assert field in entry, f"{gid} missing field {field!r}"
        assert entry["id"] == gid
        for prose in ("title", "symptom", "root_cause", "wrong_pattern",
                      "correct_pattern", "remediation"):
            assert isinstance(entry[prose], str) and entry[prose].strip(), (
                f"{gid}.{prose} must be non-empty prose"
            )
        assert entry["detection"] in DETECTIONS, gid
        assert entry["frequency"] in FREQUENCIES, gid
        assert entry["verification_status"] in VERIFICATION_STATUSES, gid
        assert entry["category"] in CATEGORIES, gid
        assert isinstance(entry["applies_to"], list) and entry["applies_to"], gid


def test_provenance_carries_source_label_and_retrieval_date():
    for gid, entry in OPERATIONAL_GOTCHA_ENTRIES.items():
        prov = entry["provenance"]
        assert isinstance(prov, dict), gid
        assert prov.get("source_label"), f"{gid} provenance missing source_label"
        assert prov.get("retrieval_date"), f"{gid} provenance missing retrieval_date"


def test_ids_unique_and_sorted_helper():
    ids = valid_operational_gotcha_ids()
    assert ids == sorted(set(ids))
    assert set(ids) == set(OPERATIONAL_GOTCHA_ENTRIES)


def test_all_six_domains_and_four_starred_entries_present():
    domains = {e["category"] for e in OPERATIONAL_GOTCHA_ENTRIES.values()}
    assert EXPECTED_DOMAINS <= domains, f"missing domains: {EXPECTED_DOMAINS - domains}"
    assert STARRED_IDS <= set(OPERATIONAL_GOTCHA_ENTRIES), (
        f"missing starred entries: {STARRED_IDS - set(OPERATIONAL_GOTCHA_ENTRIES)}"
    )


def test_index_rows_are_compact():
    index = list_operational_gotchas_index()
    assert len(index) == OPERATIONAL_GOTCHA_ENTRY_COUNT
    for row in index:
        assert set(row) == {
            "id", "title", "detection", "frequency", "category", "verification_status"
        }


# ---------------------------------------------------------------------------
# Lexical ranking — relevance is deterministic
# ---------------------------------------------------------------------------


def _top_id(query, **kw):
    result = search_operational_gotchas(query, **kw)
    assert result["results"], f"expected hits for {query!r}"
    return result["results"][0]["id"]


def test_ranking_listener_test_mode():
    assert _top_id("listener test mode") == "listener_no_test_mode"


def test_ranking_parent_redeploy():
    assert _top_id("parent redeploy process call") == "process_call_parent_redeploy"


def test_ranking_tracked_field_repeating():
    assert _top_id("tracked field repeating") == "tracked_field_repeating_first_occurrence"


def test_ranking_marketplace_recipe():
    assert _top_id("marketplace recipe production ready") == "marketplace_recipes_not_production"


def test_ranking_listener_concurrency():
    assert _top_id("web services server concurrent http 500") == "wss_listener_concurrency_http_500"


def test_ranking_edi_taglist():
    assert _top_id("edi taglist segment loop elementkey") == "edi_taglist_loop_vs_segment"


def test_ranking_marketplace_recipe_filter():
    assert _top_id("marketplace recipe filter asset type") == "marketplace_recipe_search_filter"


def test_ranking_marketplace_bundle_folder():
    assert _top_id("marketplace bundle install folder") == "marketplace_bundle_install_folder_id"


def test_ranking_status_ok_for_strong_match():
    result = search_operational_gotchas("http client overrides document")
    assert result["status"] == "ok"
    assert result["_success"] is True
    assert result["results"][0]["id"] == "http_client_step_overrides_document"


def test_top_k_clamped():
    # "process" matches several entries, so the clamp boundaries are observable.
    assert len(search_operational_gotchas("process")["results"]) > 1
    wide = search_operational_gotchas("process", top_k=99)
    assert len(wide["results"]) <= 10
    # Explicit 0 / negative clamp to the documented minimum of 1, not the default.
    assert len(search_operational_gotchas("process", top_k=0)["results"]) == 1
    assert len(search_operational_gotchas("process", top_k=-5)["results"]) == 1
    # None falls back to the default (5), then clamps.
    assert len(search_operational_gotchas("process", top_k=None)["results"]) <= 5


def test_results_carry_full_provenance_and_verification():
    result = search_operational_gotchas("listener test mode")
    for hit in result["results"]:
        assert hit["provenance"]["source_label"]
        assert hit["provenance"]["retrieval_date"]
        assert hit["verification_status"] in VERIFICATION_STATUSES


# ---------------------------------------------------------------------------
# issue_ids exact lookup — deterministic, order-preserving, never fabricates
# ---------------------------------------------------------------------------


def test_issue_ids_exact_lookup_preserves_caller_order():
    ids = ["cdc_partial_dataset_mass_delete", "listener_no_test_mode"]
    result = search_operational_gotchas(issue_ids=ids)
    assert result["mode"] == "issue_ids"
    assert result["status"] == "ok"
    assert [h["id"] for h in result["results"]] == ids


def test_issue_ids_precedence_over_query():
    result = search_operational_gotchas(
        "listener", issue_ids=["marketplace_recipes_not_production"]
    )
    assert result["mode"] == "issue_ids"
    assert [h["id"] for h in result["results"]] == ["marketplace_recipes_not_production"]


def test_issue_ids_unknown_reports_missing_and_does_not_fabricate():
    result = search_operational_gotchas(issue_ids=["__nope__"])
    assert result["status"] == "no_match"
    assert result["_success"] is False
    assert result["results"] == []
    assert result["missing_issue_ids"] == ["__nope__"]


def test_issue_ids_mixed_known_and_unknown():
    result = search_operational_gotchas(
        issue_ids=["process_call_parent_redeploy", "__nope__"]
    )
    assert result["status"] == "ok"
    assert [h["id"] for h in result["results"]] == ["process_call_parent_redeploy"]
    assert result["missing_issue_ids"] == ["__nope__"]


def test_issue_ids_accepts_single_string():
    result = search_operational_gotchas(issue_ids="groovy_compiles_first_execution")
    assert [h["id"] for h in result["results"]] == ["groovy_compiles_first_execution"]


# ---------------------------------------------------------------------------
# Empty / no-match envelopes
# ---------------------------------------------------------------------------


def test_empty_query_without_issue_ids_is_explicit():
    result = search_operational_gotchas()
    assert result["_success"] is False
    assert result["error"] == "empty_query"
    assert result["results"] == []


def test_whitespace_only_query_is_empty_query():
    result = search_operational_gotchas("   ")
    assert result["error"] == "empty_query"


def test_no_match_returns_empty_with_warning_and_no_fabrication():
    result = search_operational_gotchas("zzzz qqqq vvvv nonsense")
    assert result["status"] == "no_match"
    assert result["_success"] is False
    assert result["results"] == []
    assert result["warnings"]


def test_low_confidence_status_for_weak_match():
    # "audit" matches only the http-client remediation prose (a weight-1 field),
    # so the top score stays below the ok threshold → low_confidence + warning.
    result = search_operational_gotchas("audit")
    assert result["status"] == "low_confidence"
    assert result["_success"] is True
    assert result["warnings"]


# ---------------------------------------------------------------------------
# Anti-template lint + attribution
# ---------------------------------------------------------------------------


def test_anti_template_no_raw_artifacts():
    blob = _served_prose_blob()
    assert "```" not in blob, "code fences must not appear in gotcha prose"
    assert not _RAW_XML_RE.search(blob), "raw XML must not appear in gotcha prose"
    assert not _UUID_RE.search(blob), "UUIDs must not appear in gotcha prose"
    sql = _SQL_CRUD_RE.search(blob)
    assert sql is None, f"reusable SQL must not appear in gotcha prose: {sql!r}"
    assert "curl " not in blob, "shell/curl snippets must not appear in gotcha prose"


def test_resource_renders_attribution_and_taxonomies():
    body = render_operational_gotchas_resource()
    assert body.startswith("# Boomi Operational Gotchas")
    assert "BSD-2-Clause" in body
    for gid in OPERATIONAL_GOTCHA_ENTRIES:
        assert gid in body, f"resource missing entry {gid}"
    for token in DETECTIONS | FREQUENCIES | VERIFICATION_STATUSES:
        assert token in body, f"resource missing taxonomy token {token}"


# ---------------------------------------------------------------------------
# Scripting gotchas (Groovy custom-scripting authoring traps).
# ---------------------------------------------------------------------------

_SCRIPTING_IDS = (
    "groovy_dataprocess_storestream_required",
    "groovy_props_setproperty_null_npe",
    "groovy_ddp_prefix_required",
)


def test_scripting_category_and_entries_present():
    assert "scripting" in CATEGORIES
    for gid in _SCRIPTING_IDS:
        assert gid in OPERATIONAL_GOTCHA_ENTRIES, gid
        assert OPERATIONAL_GOTCHA_ENTRIES[gid]["category"] == "scripting"
        assert "groovy_script" in OPERATIONAL_GOTCHA_ENTRIES[gid]["applies_to"]


def test_scripting_symptoms_route_to_their_gotchas():
    assert "groovy_dataprocess_storestream_required" in triage_symptoms(
        "documents dropped after script storeStream missing"
    )
    assert "groovy_props_setproperty_null_npe" in triage_symptoms(
        "NullPointerException from setProperty null in script"
    )
    assert "groovy_ddp_prefix_required" in triage_symptoms(
        "ddp prefix document.dynamic.userdefined missing"
    )
