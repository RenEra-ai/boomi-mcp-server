"""Unit tests for the issue #82 (M9.6) plan-time script lints.

The lints are cheap, string-level WARNINGS that never block emission/planning:
  * ``SCRIPT_PROCESSING_STORE_STREAM_MISSING`` — a Data Process
    (``script.processing``) Groovy body that never calls
    ``dataContext.storeStream(...)`` silently drops documents downstream.
  * ``SCRIPT_BODY_LONG`` — any inline script body over ``_SCRIPT_BODY_MAX_LINES``
    lines crosses the Companion anti-scripting threshold.

These tests run with ``PYTHONPATH=src`` (bare ``boomi_mcp`` imports — the
editable-install ``.pth`` is stale). They prove the lints fire on the right
content, stay silent otherwise, and — critically — never create a
``validation_error`` / never change a ``planned_action`` / never reject apply.
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch

from boomi_mcp.categories import integration_builder as ib
from boomi_mcp.categories.integration_builder import (
    _SCRIPT_BODY_MAX_LINES,
    _SCRIPT_LINT_BODY_LONG,
    _SCRIPT_LINT_STORE_STREAM_MISSING,
    _collect_script_bodies,
    _lint_script_bodies,
)
from boomi_mcp.models.integration_models import (
    IntegrationComponentSpec,
    IntegrationSpecV1,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spec(*components: IntegrationComponentSpec) -> IntegrationSpecV1:
    return IntegrationSpecV1(version="1.0", name="lint-test", components=list(components))


def _codes(warnings: List[str]) -> List[str]:
    """Extract the bracketed lint code token from each warning string."""
    codes = []
    for w in warnings:
        if w.startswith("[") and "]" in w:
            codes.append(w[1 : w.index("]")])
    return codes


def _long_body(lines: int) -> str:
    return "\n".join(f"row[{i}] = i" for i in range(lines))


def _dataprocess_xml(script: str, *, name: str = "DP") -> str:
    """A raw-XML Data Process component carrying a Groovy CDATA body."""
    return (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        f'type="process" name="{name}"><bns:object><process><shapes>'
        '<shape shapetype="dataprocess" name="shape2"><configuration>'
        '<dataprocessscript language="groovy2" useCache="true">'
        f'<![CDATA[{script}]]>'
        "</dataprocessscript></configuration></shape>"
        "</shapes></process></bns:object></bns:Component>"
    )


def _raw_proc_comp(xml: str, key: str = "dp1") -> IntegrationComponentSpec:
    return IntegrationComponentSpec(
        key=key, type="script.processing", action="create", name="DP",
        config={"xml": xml},
    )


def _script_mapping_comp(
    script_body: str, key: str = "sm1"
) -> IntegrationComponentSpec:
    return IntegrationComponentSpec(
        key=key, type="script.mapping", action="create", name="My Script Map",
        config={
            "component_type": "script.mapping",
            "component_name": "My Script Map",
            "language": "groovy2",
            "script_body": script_body,
            "inputs": [{"name": "inputValue", "data_type": "character"}],
            "outputs": [{"name": "outputValue"}],
        },
    )


# ---------------------------------------------------------------------------
# storeStream lint — scoped to script.processing content only
# ---------------------------------------------------------------------------


def test_storestream_missing_in_dataprocess_is_flagged():
    xml = _dataprocess_xml("def x = dataContext.getStream(0)\n// no store back")
    warnings = _lint_script_bodies(_spec(_raw_proc_comp(xml)))
    assert _SCRIPT_LINT_STORE_STREAM_MISSING in _codes(warnings)


def test_storestream_present_in_dataprocess_is_not_flagged():
    xml = _dataprocess_xml(
        "def s = dataContext.getStream(0)\n"
        "dataContext.storeStream(s, props)"
    )
    warnings = _lint_script_bodies(_spec(_raw_proc_comp(xml)))
    assert _SCRIPT_LINT_STORE_STREAM_MISSING not in _codes(warnings)


def test_storestream_match_tolerates_incidental_whitespace():
    # `dataContext . storeStream (` is the same call; the lint must not
    # false-positive on author formatting.
    xml = _dataprocess_xml("dataContext . storeStream ( is, props )")
    warnings = _lint_script_bodies(_spec(_raw_proc_comp(xml)))
    assert _SCRIPT_LINT_STORE_STREAM_MISSING not in _codes(warnings)


def test_storestream_lint_never_fires_on_script_mapping_body():
    # A MappingScript is NOT a Data Process script — storeStream does not apply.
    comp = _script_mapping_comp("outputValue = inputValue.toUpperCase()")
    warnings = _lint_script_bodies(_spec(comp))
    assert _SCRIPT_LINT_STORE_STREAM_MISSING not in _codes(warnings)


def test_storestream_lint_ignores_non_dataprocess_raw_xml():
    # A raw-XML component with no Data Process cue is out of scope entirely.
    xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="process" name="P"><bns:object><process><shapes>'
        '<shape shapetype="connectoraction" name="shape2"/>'
        "</shapes></process></bns:object></bns:Component>"
    )
    comp = IntegrationComponentSpec(
        key="p1", type="process", action="create", name="P", config={"xml": xml}
    )
    assert _lint_script_bodies(_spec(comp)) == []


def test_script_processing_type_without_cdata_still_storestream_scanned():
    # Even when the dataprocess body isn't in a CDATA block, the whole raw XML
    # is the string-level storeStream scan target.
    xml = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
        'type="script.processing" name="DP">def x = 1; // no store</bns:Component>'
    )
    comp = _raw_proc_comp(xml)
    assert _SCRIPT_LINT_STORE_STREAM_MISSING in _codes(_lint_script_bodies(_spec(comp)))


# ---------------------------------------------------------------------------
# long-script lint — applies to every inline body
# ---------------------------------------------------------------------------


def test_long_script_mapping_body_is_flagged():
    comp = _script_mapping_comp(_long_body(_SCRIPT_BODY_MAX_LINES + 5))
    assert _SCRIPT_LINT_BODY_LONG in _codes(_lint_script_bodies(_spec(comp)))


def test_short_script_mapping_body_is_not_flagged():
    comp = _script_mapping_comp(_long_body(_SCRIPT_BODY_MAX_LINES - 1))
    assert _SCRIPT_LINT_BODY_LONG not in _codes(_lint_script_bodies(_spec(comp)))


def test_body_exactly_at_threshold_is_not_flagged():
    # > _SCRIPT_BODY_MAX_LINES, not >=, so exactly the threshold is fine.
    comp = _script_mapping_comp(_long_body(_SCRIPT_BODY_MAX_LINES))
    assert _SCRIPT_LINT_BODY_LONG not in _codes(_lint_script_bodies(_spec(comp)))


def test_long_dataprocess_cdata_body_is_flagged():
    xml = _dataprocess_xml(
        "dataContext.storeStream(is, props)\n"
        + _long_body(_SCRIPT_BODY_MAX_LINES + 5)
    )
    warnings = _lint_script_bodies(_spec(_raw_proc_comp(xml)))
    codes = _codes(warnings)
    assert _SCRIPT_LINT_BODY_LONG in codes
    # storeStream IS present here, so only the long-script warning fires.
    assert _SCRIPT_LINT_STORE_STREAM_MISSING not in codes


def test_long_script_lint_does_not_count_xml_structure_lines():
    # A raw-XML dataprocess with a SHORT CDATA body but many structural XML
    # lines must NOT be flagged long — only the embedded script is sized.
    multiline_xml_wrapper = (
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/"\n'
        + "\n".join(f'  attr{i}="v{i}"' for i in range(_SCRIPT_BODY_MAX_LINES + 10))
        + '\n type="script.processing" name="DP">'
        "<configuration><dataprocessscript>"
        "<![CDATA[dataContext.storeStream(is, props)]]>"
        "</dataprocessscript></configuration></bns:Component>"
    )
    comp = _raw_proc_comp(multiline_xml_wrapper)
    assert _SCRIPT_LINT_BODY_LONG not in _codes(_lint_script_bodies(_spec(comp)))


# ---------------------------------------------------------------------------
# collector edge cases — pure / never raises
# ---------------------------------------------------------------------------


def test_collector_handles_missing_config():
    comp = IntegrationComponentSpec(key="x", type="process", action="create", name="x")
    assert _collect_script_bodies(comp) == []


def test_collector_picks_up_nested_map_script_bodies():
    comp = IntegrationComponentSpec(
        key="map1", type="transform.map", action="create", name="Map",
        config={"script_mappings": [{"script_body": _long_body(60)}]},
    )
    bodies = _collect_script_bodies(comp)
    assert len(bodies) == 1
    assert bodies[0][1] == "mapping"
    assert _SCRIPT_LINT_BODY_LONG in _codes(_lint_script_bodies(_spec(comp)))


def test_clean_spec_emits_no_lint_warnings():
    comp = _script_mapping_comp("outputValue = inputValue.toUpperCase()")
    assert _lint_script_bodies(_spec(comp)) == []


# ---------------------------------------------------------------------------
# Plan-level proof: lints WARN but NEVER block
# ---------------------------------------------------------------------------


def _config(spec: IntegrationSpecV1, **extra: Any) -> Dict[str, Any]:
    cfg = {"conflict_policy": "reuse", "integration_spec": spec.model_dump()}
    cfg.update(extra)
    return cfg


def test_build_plan_surfaces_lint_warning_without_blocking():
    long_comp = _script_mapping_comp(_long_body(_SCRIPT_BODY_MAX_LINES + 5))
    miss_comp = _raw_proc_comp(_dataprocess_xml("def x = 1 // no store"))
    spec = _spec(long_comp, miss_comp)

    with patch.object(ib, "paginate_metadata", return_value=[]):
        plan = ib._build_plan(None, _config(spec))

    assert plan["_success"] is True
    warning_codes = _codes(plan["warnings"] or [])
    assert _SCRIPT_LINT_BODY_LONG in warning_codes
    assert _SCRIPT_LINT_STORE_STREAM_MISSING in warning_codes
    # The lint never converts a step to an error and never attaches a
    # validation_error — every step plans as a clean create.
    for step in plan["steps"]:
        assert step.get("validation_error") is None
        assert step["planned_action"] == "create"


def test_apply_plan_does_not_reject_on_lints():
    long_comp = _script_mapping_comp(_long_body(_SCRIPT_BODY_MAX_LINES + 5))
    miss_comp = _raw_proc_comp(_dataprocess_xml("def x = 1 // no store"))
    spec = _spec(long_comp, miss_comp)

    with patch.object(ib, "paginate_metadata", return_value=[]), patch.object(
        ib, "_execute_component",
        return_value={"_success": True, "component_id": "new-id"},
    ):
        result = ib._apply_plan(None, "dev", _config(spec, dry_run=False))

    # Apply ran to completion (no fail-fast on unresolvable steps) — the lints
    # never enter the unresolvable_steps gate.
    assert result.get("_success") is True
    assert "unresolvable_steps" not in result
