"""#146 (M12.11): served schemas match the runtime models, exactly.

The acceptance criterion is "exact strict schemas … match runtime wrapper
validation". The only way to hold that is to GENERATE the served schema from the
same model the wrapper validates against — so these tests compare the two rather
than pinning a hand-written snapshot, which would drift the moment a model
changed and would prove only that the snapshot was updated.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _m12_11_support import walk_keys, walk_strings  # noqa: E402
from boomi_mcp.authoring.contract import (  # noqa: E402
    authoring_schema_selectors,
    schema_version_for,
)
from boomi_mcp.categories.meta_tools import (  # noqa: E402
    _get_authoring_schema_by_name,
    _valid_schema_names,
    get_schema_template_action,
)
from boomi_mcp.errors import AUTHORING_SCHEMA_VERSION_UNAVAILABLE  # noqa: E402
from boomi_mcp.models import authoring_workflow as models  # noqa: E402

_MODEL_FOR_SELECTOR = {
    "AuthoringRequestV1": models.AuthoringRequestV1,
    "AuthoringPlanResultV1": models.AuthoringPlanResultV1,
    "AuthoringCompileResultV1": models.AuthoringCompileResultV1,
    "AuthoringRevisionBindingV1": models.AuthoringRevisionBindingV1,
    "AuthoringBuildProvenanceV1": models.AuthoringBuildProvenanceV1,
}


@pytest.mark.parametrize("selector", sorted(_MODEL_FOR_SELECTOR))
def test_the_served_schema_is_the_runtime_model_schema(selector):
    served = _get_authoring_schema_by_name(selector)
    assert served["_success"] is True
    assert served["json_schema"] == _MODEL_FOR_SELECTOR[selector].model_json_schema()


@pytest.mark.parametrize("selector", sorted(authoring_schema_selectors()))
def test_every_declared_selector_is_actually_servable(selector):
    """A selector the manifest advertises but cannot serve is worse than none."""
    served = _get_authoring_schema_by_name(selector)
    assert served["_success"] is True, selector
    assert served["json_schema"], selector
    assert served["schema_version"] == schema_version_for(selector)
    assert served["read_only"] is True
    assert served["boomi_mutation"] is False
    assert served["raw_xml_exposed"] is False


@pytest.mark.parametrize("selector", sorted(authoring_schema_selectors()))
def test_every_selector_is_discoverable(selector):
    assert selector in _valid_schema_names()


def test_the_authoring_workflow_selector_is_discoverable_and_servable():
    assert "authoring_workflow" in _valid_schema_names()
    served = _get_authoring_schema_by_name("authoring_workflow")
    assert served["_success"] is True
    assert len(served["phases"]) == 8


def test_only_apply_is_declared_as_mutating():
    """The phase boundary IS the contract this issue adds."""
    phases = _get_authoring_schema_by_name("authoring_workflow")["phases"]
    mutating = [phase["step"] for phase in phases if phase["mutates_boomi"]]
    assert mutating == [7]
    for phase in phases:
        if "action='plan'" in phase["call"] or "action='compile'" in phase["call"]:
            assert phase["mutates_boomi"] is False


def test_a_bare_selector_stays_valid_and_a_pinned_version_resolves():
    """Adding version selection must not break a one-argument call."""
    bare = _get_authoring_schema_by_name("AuthoringRequestV1")
    pinned = _get_authoring_schema_by_name("AuthoringRequestV1@1")
    assert bare["_success"] is True and pinned["_success"] is True
    assert bare["json_schema"] == pinned["json_schema"]


def test_an_unserved_version_is_refused_with_the_supported_list():
    refused = _get_authoring_schema_by_name("AuthoringRequestV1@99")
    assert refused["_success"] is False
    assert refused["error_code"] == AUTHORING_SCHEMA_VERSION_UNAVAILABLE
    assert refused["supported_versions"] == ["1"]
    assert refused["supported_versions"] == sorted(refused["supported_versions"])


def test_the_typed_request_rejects_an_unknown_field():
    """``extra="forbid"`` is what makes the served schema honest."""
    payload = {
        "contract_version": "1",
        "intent": {
            "intent_kind": "process_ir",
            "integration_name": "x",
            "component_key": "p",
            "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": []}},
        },
        "smuggled": 1,
    }
    with pytest.raises(Exception):
        models.AuthoringRequestV1.model_validate(payload)


@pytest.mark.parametrize(
    "selector",
    [
        "IntegrationSpecV1",
        "workflow_sequences",
        "design_doctrine",
        "account_governance",
        "recipe_contributions",
        "recipe_registry",
        "compose_archetypes",
        "document_cache",
        "process_property",
        "api_service",
    ],
)
def test_existing_selectors_are_untouched(selector):
    """#146 adds selectors; it does not edit the bodies of the existing ones."""
    served = get_schema_template_action(schema_name=selector)
    assert served["_success"] is True
    assert served["schema_name"] == selector
    # The new metadata rides on NEW selectors only.
    assert "revision_binding" not in served


def test_schema_hashes_are_deterministic_across_calls():
    first = _get_authoring_schema_by_name("AuthoringRequestV1")["schema_hash"]
    second = _get_authoring_schema_by_name("AuthoringRequestV1")["schema_hash"]
    assert first == second
    assert first.startswith("sha256:") and len(first) == len("sha256:") + 64


def test_two_different_schemas_have_different_hashes():
    """Guard the guard: one constant hash for everything would pass the pin above."""
    a = _get_authoring_schema_by_name("AuthoringRequestV1")["schema_hash"]
    b = _get_authoring_schema_by_name("AuthoringPlanResultV1")["schema_hash"]
    assert a != b


@pytest.mark.parametrize("selector", sorted(authoring_schema_selectors()))
def test_no_served_schema_carries_a_credential_shaped_property_or_a_path(selector):
    served = _get_authoring_schema_by_name(selector)
    forbidden = (
        "password",
        "secret",
        "credential",
        "authorization",
        "api_key",
        "access_token",
    )
    for path, key in walk_keys(served):
        lowered = key.lower()
        # `properties` is the JSON-Schema keyword whose CHILDREN are field names;
        # matching on the keyword itself would be meaningless.
        if any(token in lowered for token in forbidden):
            raise AssertionError(f"{selector}: credential-shaped key at {path}.{key}")
    for path, value in walk_strings(served):
        assert not value.startswith("/Users/"), f"{selector}: absolute path at {path}"
        assert "/src/boomi_mcp/" not in value, f"{selector}: source path at {path}"
