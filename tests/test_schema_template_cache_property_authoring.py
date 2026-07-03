"""Issue #124 (M11.5, epic #118) — planner/evidence surface tests.

The cache_property_authoring surface must carry an honest evidence ledger
(per-term provenance + named gates), and plan_integration_design must route
cache/property/state intents to the M11 doctrine entries with provenance
attached — never presenting an uncorroborated companion claim as
authoritative.
"""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from boomi_mcp.categories.meta_tools import (
    get_schema_template_action,
    plan_integration_design_action,
)
from boomi_mcp.kb.design_doctrine import DESIGN_DOCTRINE_ENTRIES


def _surface():
    result = get_schema_template_action(schema_name="cache_property_authoring")
    assert result["_success"] is True
    return result


def test_every_term_carries_a_provenance_label():
    surface = _surface()
    labels = set(surface["provenance_labels"])
    assert labels == {"live_verified", "docs_corroborated", "companion_unverified"}
    for name, term in surface["terms"].items():
        assert term.get("provenance") in labels, name


def test_no_companion_unverified_term_is_marked_executable():
    # The honesty contract: an executable term must rest on live or
    # docs-corroborated evidence, never on a companion-only claim.
    for name, term in _surface()["terms"].items():
        if term["capability_status"] == "executable":
            assert term["provenance"] in ("live_verified", "docs_corroborated"), name


def test_evidence_gates_name_every_open_gate():
    gates = _surface()["evidence_gates"]
    assert set(gates) == {
        "keyed_cache_get",
        "definedparameter_source",
        "set_process_property_step",
        "document_property_cache_key",
        "non_profiled_caches",
    }
    for reason in gates.values():
        assert reason.startswith("gated")


def test_state_scope_selection_doctrine_entry_exists_live_verified():
    entry = DESIGN_DOCTRINE_ENTRIES["state_scope_selection"]
    assert entry["capability_status"] == "emittable_today"
    assert entry["provenance"] == "live_verified"
    assert "caching_lookup_join" in entry["cross_refs"]


def test_plan_integration_design_routes_cache_state_intents():
    result = plan_integration_design_action(
        intent_flags=["cache", "state", "property", "join", "branch_handoff", "enrichment"]
    )
    assert result["_success"] is True
    names = [p["name"] for p in result["recommended_doctrine_patterns"]]
    assert "state_scope_selection" in names
    assert "caching_lookup_join" in names
    # The brief carries the rationale fields the agent needs to choose.
    for pattern in result["recommended_doctrine_patterns"]:
        assert pattern["capability_status"]
        assert pattern["when_to_use"]


def test_plan_integration_design_single_cache_flag_still_routes():
    result = plan_integration_design_action(intent_flags=["cache"])
    assert result["_success"] is True
    names = [p["name"] for p in result["recommended_doctrine_patterns"]]
    assert "caching_lookup_join" in names
