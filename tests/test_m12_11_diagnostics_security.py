"""#146 (M12.11): deterministic ordering, secret exclusion, and terminology.

Three acceptance criteria that are easy to claim and easy to lose:

* every structured collection is deterministically ordered;
* no response carries a credential, header, connection property or raw XML;
* PipelineSpec stages, the ProcessIR CFG, ComponentPlan dependencies and topology
  relations are named DISTINCTLY, and never all "flow".

The terminology assertion is scoped to what #146 introduces. ``IntegrationSpecV1.flows``
and the ``flow_sequence`` process-config key are pre-existing frozen legacy names;
a blanket "nothing is called flow" rule would fail on correct, untouched code and
would be an argument for renaming a frozen surface this issue is not renaming.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import (  # noqa: E402
    appliable_request,
    process_ir_request,
    walk_keys,
    walk_strings,
)
from boomi_mcp.authoring.contract import authoring_schema_selectors  # noqa: E402
from boomi_mcp.authoring.workflow import (  # noqa: E402
    compile_authoring_request_v1,
    plan_authoring_request_v1,
)
from boomi_mcp.categories.meta_tools import (  # noqa: E402
    _get_authoring_schema_by_name,
)
from boomi_mcp.models.authoring_workflow import (  # noqa: E402
    AuthoringDiagnosticV1,
    CapabilityGapV1,
    RequiredDecisionV1,
    ResolvedReferenceSummaryV1,
    sort_authoring_diagnostics,
    sort_by_key,
)

_CREDENTIAL_KEYS = (
    "password",
    "secret",
    "token",
    "credential",
    "authorization",
    "api_key",
    "apikey",
    "access_key",
    "private_key",
    "connection_string",
)


@pytest.fixture(scope="module")
def responses():
    """One of every new response shape, dumped to JSON."""
    plan, _ = plan_authoring_request_v1(
        process_ir_request(), profile="qa_profile", account_id="qa_account"
    )
    compiled, _ = compile_authoring_request_v1(
        process_ir_request(), profile="qa_profile", account_id="qa_account"
    )
    applied_plan, _ = plan_authoring_request_v1(
        appliable_request(), profile="qa_profile", account_id="qa_account"
    )
    return {
        "plan": plan.model_dump(mode="json"),
        "compile": compiled.model_dump(mode="json"),
        "appliable_plan": applied_plan.model_dump(mode="json"),
    }


# ---------------------------------------------------------------------------
# ordering
# ---------------------------------------------------------------------------


def test_diagnostic_ordering_is_total_and_stable():
    """Shuffling the input must not change the output."""
    items = tuple(
        AuthoringDiagnosticV1(code=code, severity=severity, path=path)
        for code, severity, path in [
            ("Z", "advisory", "/a"),
            ("A", "error", "/b"),
            ("A", "error", "/a"),
            ("M", "warning", "/c"),
            ("A", "warning", "/a"),
        ]
    )
    ordered = sort_authoring_diagnostics(items)
    assert sort_authoring_diagnostics(tuple(reversed(items))) == ordered
    assert [d.severity for d in ordered] == [
        "error",
        "error",
        "warning",
        "warning",
        "advisory",
    ]


@pytest.mark.parametrize(
    "model,kwargs_list",
    [
        (
            CapabilityGapV1,
            [
                {"capability_id": "b", "state": "gated", "path": "/2"},
                {"capability_id": "a", "state": "gated", "path": "/2"},
                {"capability_id": "a", "state": "gated", "path": "/1"},
            ],
        ),
        (
            RequiredDecisionV1,
            [
                {"decision_id": "b", "path": "/2"},
                {"decision_id": "a", "path": "/2"},
                {"decision_id": "a", "path": "/1"},
            ],
        ),
        (
            ResolvedReferenceSummaryV1,
            [
                {"ref": "$ref:b", "component_type": "process"},
                {"ref": "$ref:a", "component_type": "process"},
                {"ref": "$ref:a", "component_type": "connector-settings"},
            ],
        ),
    ],
)
def test_every_collection_sorts_by_its_declared_key(model, kwargs_list):
    items = tuple(model(**kwargs) for kwargs in kwargs_list)
    ordered = sort_by_key(items)
    assert [item.sort_key for item in ordered] == sorted(
        item.sort_key for item in items
    )
    assert sort_by_key(tuple(reversed(items))) == ordered


def test_live_responses_carry_ordered_collections(responses):
    for name, payload in responses.items():
        for field in ("errors", "warnings"):
            codes = [(d["severity"], d["code"], d["path"]) for d in payload[field]]
            severity_rank = {"error": 0, "warning": 1, "advisory": 2}
            keyed = [(severity_rank[s], c, p) for s, c, p in codes]
            assert keyed == sorted(keyed), f"{name}.{field}"
        gaps = [(g["capability_id"], g["path"]) for g in payload["capability_gaps"]]
        assert gaps == sorted(gaps), f"{name}.capability_gaps"
        refs = [
            (r["ref"], r["component_type"]) for r in payload["resolved_references"]
        ]
        assert refs == sorted(refs), f"{name}.resolved_references"


def test_component_dependencies_are_ordered():
    from boomi_mcp.authoring.workflow import build_component_dependencies
    from boomi_mcp.models.integration_models import (
        IntegrationComponentSpec,
        IntegrationSpecV1,
    )

    spec = IntegrationSpecV1(
        name="ordering",
        components=[
            IntegrationComponentSpec(key="z", type="process", depends_on=["b", "a"]),
            IntegrationComponentSpec(key="a", type="connector-settings"),
            IntegrationComponentSpec(key="b", type="connector-settings"),
        ],
    )
    edges = build_component_dependencies(spec)
    assert [(e.component_key, e.depends_on) for e in edges] == [("z", "a"), ("z", "b")]


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


def test_no_response_carries_a_credential_shaped_key(responses):
    for name, payload in responses.items():
        for path, key in walk_keys(payload):
            lowered = key.lower()
            for token in _CREDENTIAL_KEYS:
                assert token not in lowered, f"{name}: {path}.{key}"


def test_no_response_carries_raw_xml(responses):
    for name, payload in responses.items():
        for path, value in walk_strings(payload):
            assert "<bns:" not in value, f"{name}: {path}"
            assert "<?xml" not in value, f"{name}: {path}"


def test_no_response_carries_a_filesystem_path(responses):
    for name, payload in responses.items():
        for path, value in walk_strings(payload):
            assert not value.startswith("/Users/"), f"{name}: {path}"
            assert "/src/boomi_mcp/" not in value, f"{name}: {path}"


def test_the_account_scope_hash_is_the_only_scope_trace(responses):
    """Neither the profile name nor an account id may appear anywhere."""
    for name, payload in responses.items():
        for path, value in walk_strings(payload):
            assert "qa_profile" not in value, f"{name}: {path}"
            assert "qa_account" not in value, f"{name}: {path}"
        assert payload["revision_binding"]["account_scope_hash"].startswith("sha256:")


@pytest.mark.parametrize("selector", sorted(authoring_schema_selectors()))
def test_no_served_schema_example_carries_a_credential(selector):
    served = _get_authoring_schema_by_name(selector)
    for path, key in walk_keys(served.get("json_schema", {})):
        lowered = key.lower()
        for token in _CREDENTIAL_KEYS:
            assert token not in lowered, f"{selector}: {path}.{key}"


# ---------------------------------------------------------------------------
# terminology
# ---------------------------------------------------------------------------


def test_the_four_graph_shaped_things_are_named_distinctly(responses):
    """Four different structures, four different names — the issue's criterion."""
    for name, payload in responses.items():
        for field in (
            "pipeline_stages",
            "process_cfg",
            "component_dependencies",
            "topology_relations",
        ):
            assert field in payload, f"{name}: {field}"


def test_no_new_response_field_is_called_flow(responses):
    """Scoped to what #146 introduces.

    ``IntegrationSpecV1.flows`` is reachable from here (it is inside the
    ComponentPlan preview) and is frozen legacy surface, so it is exempted BY
    NAME rather than by a prefix rule that would quietly exempt a new offender.
    """
    legacy_exemptions = {"flows", "flow_sequence"}
    for name, payload in responses.items():
        for path, key in walk_keys(payload):
            if key in legacy_exemptions:
                assert "integration_spec_preview" in path, (
                    f"{name}: legacy '{key}' appeared outside the frozen "
                    f"IntegrationSpecV1 echo at {path}"
                )
                continue
            assert "flow" not in key.lower(), f"{name}: {path}.{key}"
            assert key.lower() != "graph", f"{name}: {path}.{key}"


def test_the_workflow_schema_publishes_the_four_names():
    """#146's four names are still published — and the set is allowed to GROW.

    The original assertion was equality on exactly four keys, which made a
    served vocabulary that legitimately grew read as a regression: #153 added
    five terms (process units, the relocatable materialization plan, late
    binding, and the two separate attestations) that the workflow this contract
    describes now genuinely has. Subset, not equality, keeps what this test was
    protecting — that none of the four was quietly dropped or left blank — while
    letting the contract describe what the server actually does.
    """
    terminology = _get_authoring_schema_by_name("authoring_workflow")["terminology"]
    assert {
        "pipeline_stages",
        "process_cfg",
        "component_dependencies",
        "topology_relations",
    } <= set(terminology), sorted(terminology)
    for description in terminology.values():
        assert description


def test_the_new_result_schemas_do_not_introduce_a_flow_field():
    for selector in ("AuthoringPlanResultV1", "AuthoringCompileResultV1"):
        schema = _get_authoring_schema_by_name(selector)["json_schema"]
        properties = schema.get("properties", {})
        assert "flow" not in properties
        assert "flows" not in properties
        for field in (
            "pipeline_stages",
            "process_cfg",
            "component_dependencies",
            "topology_relations",
        ):
            assert field in properties, f"{selector}: {field}"
