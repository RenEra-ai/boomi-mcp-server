"""#146 (M12.11): wrapper signatures, action parity, and backward compatibility.

The census this issue's research gate demanded, expressed as assertions. Its
whole point is that ONE contract registry feeds discovery, the dispatcher, the
workflow schema and the wrapper docstrings — so the four cannot advertise
different action lists the way the live service once advertised four archetypes
against a checkout that had six.
"""

import inspect
import os
import sys

import pytest
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

# Same preamble every server-importing test in this repo uses: without it the
# module resolves the CLOUD secret backend and raises on a missing GCP project.
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

from boomi_mcp.authoring.contract import (  # noqa: E402
    AUTHORING_ACTIONS,
    build_authoring_contract_manifest,
)
from boomi_mcp.categories import integration_builder, meta_tools  # noqa: E402
from boomi_mcp.models.authoring_workflow import AuthoringRequestV1  # noqa: E402


def test_the_action_set_is_exactly_the_legacy_three_plus_compile():
    """``compile`` is ADDITIVE. Removing or reordering an existing action would
    break every caller that switches on it."""
    assert AUTHORING_ACTIONS == ("plan", "compile", "apply", "verify")
    for legacy in ("plan", "apply", "verify"):
        assert legacy in AUTHORING_ACTIONS


def test_build_integration_wrapper_signature_is_unchanged():
    """The strongest backward-compatibility statement available: byte-identical.

    #146 carries its typed input inside the existing ``config`` JSON rather than
    as new parameters, precisely so this assertion can be made.
    """
    signature = inspect.signature(server.build_integration)
    assert list(signature.parameters) == ["profile", "action", "config"]
    assert signature.parameters["config"].default is None
    assert signature.parameters["profile"].default is inspect.Parameter.empty
    assert signature.parameters["action"].default is inspect.Parameter.empty


def test_list_capabilities_gained_exactly_one_trailing_optional_parameter():
    signature = inspect.signature(server.list_capabilities)
    assert list(signature.parameters) == ["expected_capability_revision"]
    assert signature.parameters["expected_capability_revision"].default is None


def test_build_from_archetype_is_untouched():
    assert list(inspect.signature(server.build_from_archetype).parameters) == [
        "name",
        "parameters",
        "recipe_version",
    ]


#: The signatures #146's original scope shipped. The amendment may only APPEND
#: to these — a caller who wrote against the shipped wrapper keeps every
#: positional meaning, and every new argument is optional with a ``None``
#: default. Kept as literals so an accidental reorder is a failure, not a
#: recomputation that agrees with itself.
_SHIPPED_LEADING_PARAMS = {
    "plan_integration_design": ["archetype", "intent_flags", "profile"],
    "get_schema_template": [
        "resource_type",
        "operation",
        "standard",
        "component_type",
        "protocol",
        "schema_name",
    ],
}

#: What the #146 amendment appends, in order.
_AMENDMENT_TRAILING_PARAMS = {
    "plan_integration_design": ["authoring_mode"],
    "get_schema_template": [
        "authoring_entry_id",
        "node_kind",
        "category",
        "capability_id",
        "workflow_stage",
        "after_entry_id",
        "limit",
    ],
}


@pytest.mark.parametrize("wrapper", sorted(_SHIPPED_LEADING_PARAMS))
def test_the_amendment_only_appends_trailing_optional_parameters(wrapper):
    """Backward compatibility as a POSITIONAL claim, not just a name claim.

    Asserting the parameter set is unchanged would have been wrong (it did
    change); asserting only that the new names exist would miss a reorder that
    silently changes what a positional call means. So: the shipped names must
    still LEAD, in their original order, and everything after them must be
    optional with a ``None`` default.
    """
    signature = inspect.signature(getattr(server, wrapper))
    params = list(signature.parameters)
    leading = _SHIPPED_LEADING_PARAMS[wrapper]
    trailing = _AMENDMENT_TRAILING_PARAMS[wrapper]

    assert params[: len(leading)] == leading
    assert params == leading + trailing

    for name in trailing:
        assert signature.parameters[name].default is None, name


def test_every_surface_advertises_the_same_action_set():
    """Wrapper docstring, capability catalog, workflow schema, dispatcher hint.

    Four independent renderings of one fact. Before #146 they were four
    independent literals, which is how they drift.
    """
    catalog = meta_tools.list_capabilities_action()
    assert tuple(catalog["tools"]["build_integration"]["actions"]) == AUTHORING_ACTIONS

    workflow = meta_tools._get_authoring_schema_by_name("authoring_workflow")
    assert tuple(workflow["actions"]) == AUTHORING_ACTIONS

    manifest = build_authoring_contract_manifest()
    assert tuple(manifest["actions"]) == AUTHORING_ACTIONS

    hint = integration_builder.build_integration_action(
        None, "p", "not_an_action", config={}
    )["hint"]
    for action in AUTHORING_ACTIONS:
        assert action in hint

    doc = server.build_integration.__doc__
    assert "plan, compile, apply, verify" in doc


def test_the_dispatcher_actually_routes_every_advertised_action():
    """An advertised action that 404s is worse than one never advertised."""
    source = inspect.getsource(integration_builder.build_integration_action)
    for action in AUTHORING_ACTIONS:
        assert f'normalized_action == "{action}"' in source, action


def test_the_typed_request_carries_no_credential_or_profile_field():
    """Profile travels as its own wrapper argument, never inside the payload.

    A profile smuggled into the typed intent would let one request author against
    a scope its caller was not authenticated for.
    """
    schema = AuthoringRequestV1.model_json_schema()

    # KEY NAMES, not a blob scan — the same discipline the recipe layer's own
    # forbidden-shape scan documents. A blob scan matches the word "credentials"
    # inside a DESCRIPTION that exists precisely to say credentials are
    # forbidden, so it fails on correct code and teaches nothing.
    forbidden = ("credential", "password", "api_key", "authorization", "secret")
    offenders = []
    for definition in list(schema.get("$defs", {}).values()) + [schema]:
        for property_name in (definition.get("properties") or {}):
            lowered = property_name.lower()
            if any(token in lowered for token in forbidden):
                offenders.append(property_name)
    assert offenders == [], offenders

    for field in AuthoringRequestV1.model_fields:
        assert "profile" not in field.lower()
        assert "account" not in field.lower()


def test_plan_integration_design_output_schema_required_list_is_unchanged():
    """#146's two new properties are additive and NOT required.

    Adding a required property is a breaking change for every caller validating
    against this schema — and this is the one tool in the repo that declares an
    output schema at all.
    """
    assert meta_tools.PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["required"] == [
        "_success",
        "tool",
        "mode",
        "read_only",
        "boomi_mutation",
        "raw_xml_exposed",
        "text",
    ]
    properties = meta_tools.PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA["properties"]
    assert "revision_binding" in properties
    assert "typed_next_steps" in properties


def test_the_wrapper_still_declares_the_module_level_output_schema():
    tool = server.plan_integration_design
    assert (
        getattr(tool, "output_schema", None) is None
        or tool.output_schema is meta_tools.PLAN_INTEGRATION_DESIGN_OUTPUT_SCHEMA
    )


def test_a_legacy_request_never_enters_the_typed_path():
    """Routing is by an EXPLICIT authoring_request key, never inferred.

    Silently upgrading legacy request semantics is out of scope by name in the
    issue, and it is the failure mode that would break existing callers without
    any of them changing a line.
    """
    source = inspect.getsource(integration_builder.build_integration_action)
    assert "payload = _authoring_payload(cfg)" in source
    assert "elif payload is not None:" in source


def test_the_mutation_certainty_vocabulary_is_published():
    """QA #424. An enum's vocabulary is not self-describing the way a boolean
    is, so a caller cannot branch on values no served surface ever names."""
    doc = server.build_integration.__doc__
    for value in ("performed", "possible", "none"):
        assert f'mutation_status="{value}"' in doc, value
    assert "mutation_performed" in doc


def test_the_workflow_doc_still_describes_all_eight_steps():
    """QA #422. A doc edit over-deleted the verify row and the phase-distinction
    section while the title still promised them."""
    from pathlib import Path

    doc = (
        Path(__file__).resolve().parent.parent
        / "docs" / "architecture" / "AUTHORING_WORKFLOW_V1.md"
    ).read_text()
    for step in range(1, 9):
        assert f"| {step} |" in doc, step
    assert "Three planning concepts" in doc
    assert "no `build_id`" in doc


def test_the_retry_safety_signal_is_the_status_not_one_error_code():
    """QA #425. `error_code` is a setdefault, so a more specific refusal code
    wins. Documenting one token in text that invites branching would make a
    client miss three equally retry-safe refusals."""
    doc = server.build_integration.__doc__
    assert "Branch on mutation_status, not on one" in doc
    for code in (
        "AUTHORING_PLAN_STALE",
        "AUTHORING_CAPABILITY_REVISION_MISMATCH",
        "AUTHORING_APPLY_VALIDATION_REQUIRED",
    ):
        assert code in doc, code


def test_the_served_status_contract_matches_what_the_classifier_does():
    """Codex review (round 4), P2. The docstring defined `performed` as "returned
    a component id" while the classifier requires the step to have SUCCEEDED —
    so a failed update with a target id is `possible`-with-ID, and a client
    following the served text would have mishandled it.

    Derived from the classifier, not restated: this asserts the SHAPES the
    implementation actually produces are the ones the description documents.
    """
    from boomi_mcp.categories.integration_builder import _mutation_status

    failed_update_with_id = {
        "partial_results": {
            "a": {
                "status": "updated",
                "component_id": "target-1",
                "result": {"_success": False, "retryable": True},
            }
        }
    }
    # A SUCCESSFUL apply returns `results`, not `partial_results` — the latter
    # is emitted only on failure. Modelling this sub-case under the failure key
    # meant the assertion never exercised the shape it documents, and would have
    # kept passing if the classifier stopped reading `results` at all.
    succeeded_without_id = {
        "_success": True,
        "results": {
            "a": {"status": "created", "component_id": None,
                  "result": {"_success": True}}
        },
    }
    assert _mutation_status(failed_update_with_id) == "possible"
    assert _mutation_status(succeeded_without_id) == "possible"

    doc = server.build_integration.__doc__
    # The served text must state the id-alone caveat, since that is exactly the
    # inference the previous wording invited.
    assert "an id alone is not enough" in doc.lower()
    assert "SUCCEEDED and returned a" in doc
    assert "succeeded without returning" in doc
    assert "partial_results[<key>].result" in doc
    # QA #429: the pointer resolves in only ONE of `possible`'s two sub-cases —
    # a SUCCEEDED apply has no partial_results at all, and its only tell is a
    # null component_id. Both routes must be readable from the served text.
    assert "results[<key>].component_id" in doc
    # "SUCCEEDED, there is no" and not the two tokens separately: both already
    # occurred in this docstring for unrelated reasons ("there is no build to
    # identify" in the compile bullet), so the loose form passed on the very
    # revision it exists to catch — zero coverage wearing the shape of a pin.
    assert "SUCCEEDED, there is no" in doc
    # The successful sub-case must not be described as a confirmed creation —
    # that is the certainty `possible` exists to withhold.
    assert "UNCONFIRMED" in doc
    assert "created but its id did not come back" not in doc


def test_the_tool_catalog_teaches_a_config_shape_that_works():
    """QA #442. The catalog listed the binding values as config-ROOT keys; they
    are fields INSIDE authoring_request and root copies are silently dropped, so
    a caller who discovered the surface the documented way — list_capabilities
    first — was steered into a guaranteed AUTHORING_APPLY_VALIDATION_REQUIRED."""
    catalog = meta_tools.list_capabilities_action()
    config_text = catalog["tools"]["build_integration"]["parameters"]["config"]
    assert "authoring_request" in config_text
    # It must say the binding values live INSIDE the typed request...
    assert "INSIDE" in config_text or "inside" in config_text
    # ...and warn that the root placement fails, which is the actual trap.
    assert "AUTHORING_APPLY_VALIDATION_REQUIRED" in config_text
    # The served docstring and the catalog must agree on that point.
    assert "not siblings of it" in server.build_integration.__doc__
