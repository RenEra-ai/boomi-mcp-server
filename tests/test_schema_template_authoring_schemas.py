"""Unit tests for get_schema_template's schema_name selector (Issue #10).

Pure-unit against boomi_mcp.categories.meta_tools — no server import, no SDK
calls. Covers the four schema_name families, error envelopes, selector
precedence, and legacy resource_type compatibility.
"""

import json
import sys
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories.meta_tools import (
    _valid_schema_names,
    get_schema_template_action,
)
from boomi_mcp.errors import (
    SCHEMA_NAME_UNSUPPORTED,
    SCHEMA_SELECTOR_REQUIRED,
    WORKFLOW_SEQUENCE_NOT_FOUND,
)


# ---------------------------------------------------------------------------
# IntegrationSpecV1
# ---------------------------------------------------------------------------


def test_integration_spec_v1_returns_json_schema():
    result = get_schema_template_action(schema_name="IntegrationSpecV1")
    assert result["_success"] is True
    assert result["schema_name"] == "IntegrationSpecV1"
    assert "components" in result["json_schema"]["properties"]
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    assert "archetype" in result["hint"]  # archetype-first pointer


# ---------------------------------------------------------------------------
# archetype:<name>
# ---------------------------------------------------------------------------


def test_archetype_schema_returns_parameter_schema_and_metadata():
    result = get_schema_template_action(schema_name="archetype:database_to_api_sync")
    assert result["_success"] is True
    assert result["schema_name"] == "archetype:database_to_api_sync"
    assert result["metadata"]["name"] == "database_to_api_sync"
    assert "properties" in result["parameter_schema"]
    assert result["example_policy"] == "example_only_not_reusable_template"
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False


def test_unknown_archetype_returns_schema_name_unsupported():
    result = get_schema_template_action(schema_name="archetype:__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_NAME_UNSUPPORTED
    assert "valid_schema_names" in result
    assert "archetype:database_to_api_sync" in result["valid_schema_names"]


# ---------------------------------------------------------------------------
# workflow_sequences / workflow:<name>
# ---------------------------------------------------------------------------


def test_workflow_sequences_returns_all_sequences_and_record_schema():
    result = get_schema_template_action(schema_name="workflow_sequences")
    assert result["_success"] is True
    assert "build_integration_from_description" in result["workflow_sequences"]
    assert result["record_schema"]["required"] == ["description", "steps"]
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False


def test_single_workflow_is_profile_first():
    result = get_schema_template_action(
        schema_name="workflow:build_integration_from_description"
    )
    assert result["_success"] is True
    assert "list_boomi_profiles" in result["workflow"]["steps"][0]


def test_unknown_workflow_returns_workflow_sequence_not_found():
    result = get_schema_template_action(schema_name="workflow:__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == WORKFLOW_SEQUENCE_NOT_FOUND
    assert "build_integration_from_description" in result["valid_workflows"]


# ---------------------------------------------------------------------------
# Selector envelope behavior
# ---------------------------------------------------------------------------


def test_unknown_schema_name_lists_valid_names():
    result = get_schema_template_action(schema_name="__bogus__")
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_NAME_UNSUPPORTED
    assert "IntegrationSpecV1" in result["valid_schema_names"]
    assert "workflow_sequences" in result["valid_schema_names"]


def test_missing_both_selectors_returns_selector_required():
    result = get_schema_template_action()
    assert result["_success"] is False
    assert result["error_code"] == SCHEMA_SELECTOR_REQUIRED
    assert "valid_types" in result
    assert "valid_schema_names" in result


def test_schema_name_takes_precedence_over_resource_type():
    result = get_schema_template_action(
        resource_type="process", schema_name="IntegrationSpecV1"
    )
    assert result["_success"] is True
    assert result["schema_name"] == "IntegrationSpecV1"


def test_process_create_returns_removal_guidance():
    # The resource_type dispatch still resolves, but process operation='create'
    # now returns removal guidance instead of the freeform shape template.
    result = get_schema_template_action(resource_type="process", operation="create")
    assert result["_success"] is True
    assert result["removed"] is True
    assert "single_process_template" not in result


def test_valid_schema_names_covers_all_families():
    names = _valid_schema_names()
    assert "IntegrationSpecV1" in names
    assert "workflow_sequences" in names
    assert "design_doctrine" in names
    assert "account_governance" in names  # issue #93
    assert any(n.startswith("workflow:") for n in names)
    assert any(n.startswith("archetype:") for n in names)
    assert any(n.startswith("design_pattern:") for n in names)
    assert any(n.startswith("governance_pattern:") for n in names)  # issue #93


def test_archetype_discovery_resolves_callers_namespace():
    """Registry discovery must derive the patterns package from the calling
    module's own namespace, not a hard-coded 'boomi_mcp.patterns' string, so
    src.boomi_mcp imports walk src.boomi_mcp.patterns. (A pre-existing absolute
    import in categories.components.builders still blocks full archetype loading
    in a src-only environment — so accept either success or a graceful envelope
    that provably attempted the caller's namespace.)
    """
    import json
    import subprocess
    import sys

    repo_root = str(Path(__file__).resolve().parent.parent)
    script = (
        "import sys, json\n"
        f"sys.path.insert(0, {repo_root!r})\n"
        "from src.boomi_mcp.categories.meta_tools import get_schema_template_action\n"
        "r = get_schema_template_action(schema_name='archetype:database_to_api_sync')\n"
        "print(json.dumps({'success': r.get('_success'),\n"
        "                  'module': (r.get('context') or {}).get('module', '')}))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, cwd="/", timeout=120,
    )
    assert proc.returncode == 0, f"dispatch crashed: {proc.stderr[-2000:]}"
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["success"] is True or result["module"].startswith(
        "src.boomi_mcp.patterns"
    ), f"discovery did not resolve the caller's namespace: {result!r}"


# ---------------------------------------------------------------------------
# script_dataprocess / script_mapping — Groovy authoring contracts
# ---------------------------------------------------------------------------


def test_script_dataprocess_schema_returns_authoring_contract():
    result = get_schema_template_action(schema_name="script_dataprocess")
    assert result["_success"] is True
    assert result["schema_name"] == "script_dataprocess"
    assert result["read_only"] is True
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    blob = json.dumps(result)
    assert "```" not in blob and "<bns:" not in blob and "<?xml" not in blob
    for token in (
        "dataContext",
        "storeStream",
        "document.dynamic.userdefined",
        "ExecutionUtil",
        "search_boomi_docs",
    ):
        assert token in blob, token
    assert "groovy_compiles_first_execution" in result["related_gotchas"]
    for gid in (
        "groovy_dataprocess_storestream_required",
        "groovy_props_setproperty_null_npe",
        "groovy_ddp_prefix_required",
    ):
        assert gid in result["related_gotchas"], gid


def test_script_mapping_schema_returns_authoring_contract():
    result = get_schema_template_action(schema_name="script_mapping")
    assert result["_success"] is True
    assert result["schema_name"] == "script_mapping"
    assert result["read_only"] is True
    assert result["raw_xml_exposed"] is False
    assert result["boomi_mutation"] is False
    blob = json.dumps(result)
    assert "```" not in blob and "<bns:" not in blob
    # storeStream is not a map-script concept — must not appear in the skeleton.
    assert "storeStream" not in result["skeleton"]
    for token in ("ExecutionUtil", "search_boomi_docs", "output"):
        assert token in blob, token
    # The create invocation must pass config as a JSON STRING (the public
    # manage_component wrapper is config: str + json.loads), carrying
    # component_type — not a top-level type= arg and not a Python dict.
    assert "config='{" in result["tool"]  # single-quoted JSON string
    assert '"component_type": "script.mapping"' in result["tool"]
    assert "type='script.mapping'" not in result["tool"]  # no bare type= arg
    assert "config={" not in result["tool"]  # not a Python dict


def test_script_schemas_listed_and_in_unknown_envelope():
    names = _valid_schema_names()
    assert "script_dataprocess" in names
    assert "script_mapping" in names
    envelope = get_schema_template_action(schema_name="__bogus__")
    assert "script_dataprocess" in envelope["valid_schema_names"]
    assert "script_mapping" in envelope["valid_schema_names"]
