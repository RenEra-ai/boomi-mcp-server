"""Tests for list_capabilities_action V3 archetype discoverability (Issue #20).

Validates that the static MCP capability catalog:
- Exposes the three V3 authoring tools under the "Integration Authoring" category.
- Marks them as read-only and no-Boomi-mutation.
- Surfaces an archetype-first workflow for integration creation with a fallback
  for the direct IntegrationSpecV1 path.
- Filters the workflow out when the authoring tools aren't registered.
- Includes a top-level hint pointing new authoring at archetypes first.
"""

import re

from boomi_mcp.categories.meta_tools import list_capabilities_action


AUTHORING_TOOLS = (
    "list_integration_archetypes",
    "get_integration_archetype",
    "build_from_archetype",
)

DOC_TOOLS = (
    "search_boomi_docs",
    "read_boomi_doc_page",
)


# ---------------------------------------------------------------------------
# Catalog membership and annotations
# ---------------------------------------------------------------------------


def test_list_capabilities_includes_three_authoring_tools():
    catalog = list_capabilities_action()
    tools = catalog["tools"]
    for name in AUTHORING_TOOLS:
        assert name in tools, f"{name} missing from list_capabilities tools"
        assert tools[name]["category"] == "Integration Authoring"


def test_authoring_tools_marked_read_only_and_no_boomi_mutation():
    tools = list_capabilities_action()["tools"]
    for name in AUTHORING_TOOLS:
        entry = tools[name]
        assert entry["read_only"] is True, f"{name} must be read_only"
        assert entry.get("no_boomi_mutation") is True, (
            f"{name} must declare no_boomi_mutation=True"
        )


def test_list_capabilities_includes_docs_kb_tools():
    catalog = list_capabilities_action()
    tools = catalog["tools"]
    for name in DOC_TOOLS:
        assert name in tools, f"{name} missing from list_capabilities tools"
        assert tools[name]["category"] == "Documentation"
        assert tools[name]["read_only"] is True


def test_docs_workflow_and_hint_point_at_search_tool():
    catalog = list_capabilities_action()
    assert "research_boomi_docs" in catalog["workflows"]
    steps = " ".join(catalog["workflows"]["research_boomi_docs"]["steps"])
    assert "search_boomi_docs" in steps
    assert "read_boomi_doc_page" in steps
    assert "boomi_docs" in catalog["hints"]
    assert "search_boomi_docs" in catalog["hints"]["boomi_docs"]


# ---------------------------------------------------------------------------
# Workflow: archetype-first with fallback
# ---------------------------------------------------------------------------


def test_workflow_starts_profile_first_then_archetypes():
    wf = list_capabilities_action()["workflows"]["build_integration_from_description"]
    assert wf["steps"], "workflow must have at least one step"
    assert "list_boomi_profiles" in wf["steps"][0], (
        f"first step must reference list_boomi_profiles, got: {wf['steps'][0]!r}"
    )
    assert "list_integration_archetypes" in wf["steps"][1], (
        f"second step must reference list_integration_archetypes, got: {wf['steps'][1]!r}"
    )


def test_workflow_chain_runs_through_archetype_to_build_integration_plan():
    """Profile → archetype discovery → get → build_from → build_integration(action='plan')."""
    wf = list_capabilities_action()["workflows"]["build_integration_from_description"]
    steps = wf["steps"]

    # Extract the tool referenced by each numbered step. This mirrors the
    # extractor used by the available_tools filter at meta_tools.py:2393–2407.
    referenced = []
    for step in steps:
        m = re.match(r"\d+\.\s+(\w+)\(", step)
        if m:
            referenced.append(m.group(1))

    # The first four numbered steps: profile first, then the archetype chain.
    assert referenced[:4] == ["list_boomi_profiles", *AUTHORING_TOOLS], (
        f"workflow must start profile-first then the archetype chain, got: {referenced[:4]!r}"
    )
    # At least one downstream step must hand off to build_integration(action='plan').
    assert any(
        "build_integration" in s and "plan" in s for s in steps
    ), "workflow must hand off to build_integration(action='plan')"


def test_workflow_preserves_direct_fallback_path():
    """The legacy direct path stays available — as un-numbered fallback prose."""
    wf = list_capabilities_action()["workflows"]["build_integration_from_description"]
    assert "fallback" in wf, "workflow must publish a fallback for shapes no archetype covers"
    fallback = wf["fallback"]
    assert fallback["steps"], "fallback must list concrete steps"
    fallback_text = " ".join(fallback["steps"])
    assert "get_schema_template" in fallback_text
    assert "build_integration" in fallback_text


# ---------------------------------------------------------------------------
# available_tools filtering
# ---------------------------------------------------------------------------


def test_authoring_workflow_dropped_when_tools_not_registered():
    """If the registry-discovered runtime is missing the authoring tools, the
    archetype-first workflow must be filtered out — the existing regex-based
    filter catches every numbered step and drops the workflow when any
    referenced tool is absent.
    """
    only = {"build_integration", "get_schema_template", "list_boomi_profiles"}
    catalog = list_capabilities_action(available_tools=only)

    # The 3 authoring tools must not appear in the filtered catalog.
    for name in AUTHORING_TOOLS:
        assert name not in catalog["tools"], (
            f"{name} should be filtered out when not in available_tools"
        )
    # The archetype-first workflow must be dropped because its steps reference
    # tools no longer in the catalog.
    assert "build_integration_from_description" not in catalog["workflows"]


def test_docs_tools_filtered_when_not_registered():
    only = {"build_integration", "get_schema_template", "list_boomi_profiles"}
    catalog = list_capabilities_action(available_tools=only)

    for name in DOC_TOOLS:
        assert name not in catalog["tools"], (
            f"{name} should be filtered out when not in available_tools"
        )
    assert "research_boomi_docs" not in catalog["workflows"]
    assert "boomi_docs" not in catalog["hints"]


def test_docs_workflow_preserved_when_docs_tools_registered():
    only = {"search_boomi_docs", "read_boomi_doc_page"}
    catalog = list_capabilities_action(available_tools=only)
    assert set(catalog["tools"]) == set(DOC_TOOLS)
    assert "research_boomi_docs" in catalog["workflows"]
    assert "boomi_docs" in catalog["hints"]


def test_authoring_workflow_preserved_when_all_referenced_tools_present():
    """When the runtime exposes the authoring chain + build_integration, the
    archetype-first workflow survives the filter."""
    only = {
        "list_boomi_profiles",
        "list_integration_archetypes",
        "get_integration_archetype",
        "build_from_archetype",
        "build_integration",
        "review_transformation",
        "orchestrate_deploy",
    }
    catalog = list_capabilities_action(available_tools=only)
    assert "build_integration_from_description" in catalog["workflows"]
    wf = catalog["workflows"]["build_integration_from_description"]
    assert "list_boomi_profiles" in wf["steps"][0]
    assert "list_integration_archetypes" in wf["steps"][1]


# ---------------------------------------------------------------------------
# Hints
# ---------------------------------------------------------------------------


def test_prefer_archetypes_hint_points_at_list_integration_archetypes():
    hints = list_capabilities_action()["hints"]
    assert "prefer_archetypes" in hints, "hints must include prefer_archetypes"
    assert "list_integration_archetypes" in hints["prefer_archetypes"]


def test_prefer_archetypes_hint_suppressed_when_authoring_tools_not_registered():
    """If list_integration_archetypes isn't in the live registry (e.g., the
    integration_authoring import failed), the hint must not recommend it."""
    only = {"build_integration", "get_schema_template", "list_boomi_profiles"}
    hints = list_capabilities_action(available_tools=only)["hints"]
    assert "prefer_archetypes" not in hints, (
        "hint must be gated on list_integration_archetypes being registered"
    )


def test_workflow_fallback_dropped_when_schema_template_absent():
    """When archetype tools are registered but get_schema_template is not, the
    main workflow must survive but the fallback (which calls get_schema_template)
    must be stripped — agents can still follow the archetype-first chain."""
    only = {
        "list_boomi_profiles",
        "list_integration_archetypes",
        "get_integration_archetype",
        "build_from_archetype",
        "build_integration",
        "review_transformation",
        "orchestrate_deploy",
    }
    catalog = list_capabilities_action(available_tools=only)
    wf = catalog["workflows"].get("build_integration_from_description")
    assert wf is not None, "main workflow must survive when archetype chain is intact"
    assert "fallback" not in wf, (
        "fallback must be stripped when get_schema_template isn't registered"
    )


def test_workflow_fallback_preserved_when_all_referenced_tools_present():
    """When every fallback tool is also registered, the fallback block must
    travel with the workflow."""
    only = {
        "list_boomi_profiles",
        "list_integration_archetypes",
        "get_integration_archetype",
        "build_from_archetype",
        "build_integration",
        "review_transformation",
        "get_schema_template",
        "orchestrate_deploy",
    }
    catalog = list_capabilities_action(available_tools=only)
    wf = catalog["workflows"]["build_integration_from_description"]
    assert "fallback" in wf, "fallback must survive when all referenced tools registered"
    fallback_text = " ".join(wf["fallback"]["steps"])
    assert "get_schema_template" in fallback_text


# ---------------------------------------------------------------------------
# Issue #47 — infer_profile_fields discoverability
# ---------------------------------------------------------------------------


def test_infer_profile_fields_in_capabilities():
    tools = list_capabilities_action()["tools"]
    assert "infer_profile_fields" in tools
    entry = tools["infer_profile_fields"]
    assert entry["category"] == "Integration Authoring"
    assert entry["read_only"] is True
    assert entry.get("no_boomi_mutation") is True


def test_infer_profile_fields_filtered_out_when_not_registered():
    only = {"build_integration"}
    tools = list_capabilities_action(available_tools=only)["tools"]
    assert "infer_profile_fields" not in tools


# ---------------------------------------------------------------------------
# Issue #64 — orchestrate_deploy discoverability
# ---------------------------------------------------------------------------


def test_orchestrate_deploy_in_capabilities():
    tools = list_capabilities_action()["tools"]
    assert "orchestrate_deploy" in tools, "orchestrate_deploy missing from list_capabilities tools"
    entry = tools["orchestrate_deploy"]
    assert entry["category"] == "Deployment & B2B"
    assert entry["read_only"] is False
    assert entry["implemented"] is True
    # The public response keys the wrapper guarantees must be documented.
    for key in ("_success", "build_id", "process_id", "environment_id", "runtime_id", "next_steps"):
        assert key in entry["response_keys"], f"{key} missing from documented response_keys"


def test_orchestrate_deploy_filtered_out_when_not_registered():
    only = {"build_integration"}
    tools = list_capabilities_action(available_tools=only)["tools"]
    assert "orchestrate_deploy" not in tools


def test_build_integration_points_to_orchestrate_deploy():
    """build_integration capability text must route apply's build_id to orchestrate_deploy."""
    entry = list_capabilities_action()["tools"]["build_integration"]
    text = entry["description"] + " " + " ".join(entry.get("examples", []))
    assert "orchestrate_deploy" in text, (
        "build_integration must point agents from apply to orchestrate_deploy"
    )


# ---------------------------------------------------------------------------
# Issue #10 — operating doctrine
# ---------------------------------------------------------------------------


DOCTRINE_KEYS = (
    "profile_first",
    "archetype_first",
    "typed_tools_before_raw",
    "raw_write_gate_enforced",
    "reuse_secured_connections",
    "review_logs_after_test",
    "bounded_escalation",
    "repeated_auth_stop",
    "gui_only_boundaries",
    "no_throwaway_scripts",
)


def test_operating_doctrine_present_with_all_entries():
    doctrine = list_capabilities_action()["operating_doctrine"]
    for key in DOCTRINE_KEYS:
        assert key in doctrine, f"operating_doctrine missing {key}"
        assert isinstance(doctrine[key], str) and doctrine[key].strip()


def test_doctrine_raw_write_gate_described_as_enforced():
    doctrine = list_capabilities_action()["operating_doctrine"]
    text = doctrine["raw_write_gate_enforced"]
    assert "confirm_write" in text
    assert "ENFORCED" in text
    assert "RAW_WRITE_CONFIRMATION_REQUIRED" in text


def test_doctrine_repeated_auth_stop_labeled_companion_unverified():
    doctrine = list_capabilities_action()["operating_doctrine"]
    assert doctrine["repeated_auth_stop"].startswith("[companion_unverified]")


def test_doctrine_present_in_filtered_catalog():
    """Doctrine is text-only guidance — it must survive available_tools filtering."""
    catalog = list_capabilities_action(available_tools={"build_integration"})
    assert "operating_doctrine" in catalog
    for key in DOCTRINE_KEYS:
        assert key in catalog["operating_doctrine"]


def test_new_doctrine_hints_present():
    hints = list_capabilities_action()["hints"]
    for key in ("raw_write_gate", "review_logs", "bounded_retries",
                "reuse_connections", "avoid_scripts"):
        assert key in hints, f"hints missing {key}"
    assert "confirm_write=true" in hints["raw_write_gate"]
