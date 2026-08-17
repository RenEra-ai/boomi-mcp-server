"""Surviving integration-envelope model invariants (re-homed by issue #151, M12.14).

These three assertions were written as issue-#135 compatibility-freeze pins, but
they characterize the SURVIVING surface — ``IntegrationSpecV1`` /
``IntegrationComponentSpec`` envelope leniency — not one of the legacy authoring
oracles that #160 retires with the ``database_to_api_sync`` dispatch entry. Left in
``tests/test_issue_135_compatibility_freeze.py`` they would be deleted along with
that suite, silently dropping coverage of a surface that is not going anywhere.

Bodies and node names are unchanged from their previous home; only their file moved.
Their two fixture cases are inlined as module literals rather than read from
``tests/fixtures/compatibility/issue_135/authoring_boundaries.json`` so this module
carries no dependency on the legacy-oracle fixture (which #160 removes with the
suite). The literals are copied verbatim from that fixture at
47b20dd776a158c6fd096779262bbadd2867b7e3.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.models.integration_models import (  # noqa: E402
    IntegrationComponentSpec,
    IntegrationSpecV1,
)

#: Inlined from the issue-135 fixture; see the module docstring.
_CASES = {
    "spec_extra_ignore": {
        "input": {
            "name": "Sentinel Spec",
            "totally_unknown_field": "sentinel-extra-value",
        },
        "unknown_key": "totally_unknown_field",
        "expected_dump": {
            "version": "1.0",
            "name": "Sentinel Spec",
            "mode": "lift_shift",
            "components": [],
            "goals": [],
            "endpoints": [],
            "flows": [],
            "naming": {},
            "folders": {},
            "runtime": {},
            "validation_rules": {},
            "profile_indexes_by_component_id": None,
            "pipeline": None,
        },
    },
    "component_extra_ignore_config_preserved": {
        "input": {
            "key": "sentinel_component",
            "type": "process",
            "unknown_component_field": "sentinel-extra-value",
            "config": {
                "process_kind": "wrapper_subprocess",
                "process_calls": [{"subprocess_ref": "$ref:child"}],
                "free_form_block": {"alpha": 1, "beta": ["x", "y"]},
            },
        },
        "unknown_key": "unknown_component_field",
    },
}


def _case(name):
    return copy.deepcopy(_CASES[name])


def test_integration_spec_defaults_serialization():
    """Pin the exact default serialization of a minimal IntegrationSpecV1."""
    assert IntegrationSpecV1(name="Sentinel").model_dump() == {
        "version": "1.0",
        "name": "Sentinel",
        "mode": "lift_shift",
        "components": [],
        "goals": [],
        "endpoints": [],
        "flows": [],
        "naming": {},
        "folders": {},
        "runtime": {},
        "validation_rules": {},
        "profile_indexes_by_component_id": None,
        "pipeline": None,
    }


def test_spec_envelope_ignores_unknown_fields():
    case = _case("spec_extra_ignore")
    spec = IntegrationSpecV1(**case["input"])
    unknown = case["unknown_key"]
    assert not hasattr(spec, unknown)
    dump = spec.model_dump()
    assert unknown not in dump
    assert dump == case["expected_dump"]


def test_component_envelope_ignores_unknown_fields_but_preserves_config():
    case = _case("component_extra_ignore_config_preserved")
    comp = IntegrationComponentSpec(**case["input"])
    unknown = case["unknown_key"]
    assert not hasattr(comp, unknown)
    assert unknown not in comp.model_dump()
    # The free-form config dict passes through verbatim (never schema-validated).
    assert comp.config == case["input"]["config"]
