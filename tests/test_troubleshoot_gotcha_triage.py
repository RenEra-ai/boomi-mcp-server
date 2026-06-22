"""Unit tests for the symptom→gotcha triage helpers (issue #78, M9.2).

These import ONLY boomi_mcp.kb.operational_gotchas — the catalog is stdlib-only,
so no Boomi SDK / ML stack is needed (mirrors tests/kb/test_operational_gotchas.py
import discipline).
"""
import os
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(str(Path(_HERE).parent), "src")
for _p in (_SRC,):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.kb.operational_gotchas import (  # noqa: E402
    OPERATIONAL_GOTCHA_ENTRIES,
    _SYMPTOM_ROUTES,
    gotcha_matches_for_symptoms,
    triage_symptoms,
)


# ---------------------------------------------------------------------------
# triage_symptoms — each documented #78 symptom routes to the expected id
# ---------------------------------------------------------------------------

# Phrase → expected gotcha id, drawn from the issue's symptom list.
_DOCUMENTED_SYMPTOMS = [
    ("the variable appears literally in the output", "env_var_literal_in_component_xml"),
    ("auth failures despite configured credentials", "env_var_literal_in_component_xml"),
    ("uniform 401 on every route", "env_var_literal_in_component_xml"),
    ("subprocess changes apparently ignored", "process_call_parent_redeploy"),
    ("404 on a deployed API", "wss_path_objectname_verbatim"),
    ("extension values disappearing after deploy", "empty_process_overrides_hides_extensions"),
    ("record silently missing from multi-record output", "edi_taglist_loop_vs_segment"),
    ("no data produced from map", "edi_taglist_loop_vs_segment"),
]


def test_triage_maps_each_documented_symptom():
    for phrase, expected_id in _DOCUMENTED_SYMPTOMS:
        ids = triage_symptoms(phrase)
        assert expected_id in ids, f"{phrase!r} should route to {expected_id!r}, got {ids!r}"


def test_triage_is_case_insensitive():
    ids = triage_symptoms("404 ON A DEPLOYED API")
    assert "wss_path_objectname_verbatim" in ids


def test_no_match_returns_empty():
    assert triage_symptoms("everything completed successfully with no errors") == []


def test_triage_handles_non_string_and_blank():
    assert triage_symptoms(None) == []          # type: ignore[arg-type]
    assert triage_symptoms("") == []
    assert triage_symptoms("   ") == []
    assert triage_symptoms(12345) == []         # type: ignore[arg-type]


def test_triage_dedupes_when_multiple_signatures_fire():
    # Two env-var signatures present in one blob → id appears once.
    ids = triage_symptoms("the variable appears literally and auth fails despite credentials")
    assert ids.count("env_var_literal_in_component_xml") == 1


def test_triage_orders_by_route_table():
    # A blob hitting several routes returns ids in _SYMPTOM_ROUTES order.
    blob = "404 error and extension values disappearing and no data produced from map"
    ids = triage_symptoms(blob)
    assert ids == [
        "wss_path_objectname_verbatim",
        "empty_process_overrides_hides_extensions",
        "edi_taglist_loop_vs_segment",
    ]


# ---------------------------------------------------------------------------
# gotcha_matches_for_symptoms — compact projection
# ---------------------------------------------------------------------------


def test_matches_projected_compactly():
    matches = gotcha_matches_for_symptoms("404 on a deployed API")
    assert len(matches) == 1
    match = matches[0]
    # Exactly the four compact keys — no full-entry prose leaks through.
    assert set(match) == {"id", "title", "remediation", "lookup"}
    assert match["id"] == "wss_path_objectname_verbatim"
    assert match["title"] == OPERATIONAL_GOTCHA_ENTRIES["wss_path_objectname_verbatim"]["title"]
    assert match["remediation"] == OPERATIONAL_GOTCHA_ENTRIES["wss_path_objectname_verbatim"]["remediation"]
    assert match["lookup"] == "search_boomi_gotchas(issue_ids=['wss_path_objectname_verbatim'])"
    # The verbose contrastive fields must NOT be surfaced in the compact match.
    assert "wrong_pattern" not in match
    assert "root_cause" not in match


def test_matches_empty_on_no_route():
    assert gotcha_matches_for_symptoms("nothing relevant here") == []


def test_matches_preserve_route_order():
    blob = "404 and no data produced from map"
    matches = gotcha_matches_for_symptoms(blob)
    ids = [m["id"] for m in matches]
    assert ids == ["wss_path_objectname_verbatim", "edi_taglist_loop_vs_segment"]


# ---------------------------------------------------------------------------
# Guardrail: every routable id must exist in the catalog (route-table drift)
# ---------------------------------------------------------------------------


def test_every_routed_id_exists_in_catalog():
    for gid, _signatures in _SYMPTOM_ROUTES:
        assert gid in OPERATIONAL_GOTCHA_ENTRIES, f"route id {gid!r} absent from catalog"
