"""Contract tests for the env-account atom-attach hardening (issue #10 P1 slice).

The six direct atom-attachment actions in manage_deployment are deprecated: on
environment-enabled accounts Boomi rejects attach_* outright and returns empty list_*
results even when bindings exist. These tests pin the hardened behavior — fail-closed
structured errors with remediation on attach, warning/hint on empty lists, deprecation
metadata everywhere — and confirm manage_runtimes attach/list_attachments (the correct
EnvironmentAtomAttachment path) is unchanged.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from boomi.net.transport.api_error import ApiError
from boomi.models import EnvironmentAtomAttachment

from boomi_mcp.categories.deployment.packages import manage_deployment_action
from boomi_mcp.categories.deployment.deployment_utils import (
    ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED,
    DEPRECATED_ATOM_ATTACHMENT_ACTION,
    atom_attachment_deprecation_metadata,
    is_environment_account_signal,
)
from boomi_mcp.categories.runtimes import manage_runtimes_action


ENV_ACCOUNT_MESSAGE = (
    "This account uses environments. Please use ComponentEnvironmentAttachment"
)


def _assert_deprecation_metadata(result, action):
    assert result["deprecated"] is True
    dep = result["deprecation"]
    assert dep["error_code"] == DEPRECATED_ATOM_ATTACHMENT_ACTION
    assert dep["deprecated_action"] == action
    assert dep["replacement_actions"]
    assert dep["note"]


# ---------------------------------------------------------------------------
# Attach on environment-enabled accounts: fail closed with remediation
# ---------------------------------------------------------------------------

def test_attach_process_atom_env_account_fails_closed():
    sdk = MagicMock()
    sdk.process_atom_attachment.create_process_atom_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="attach_process_atom",
        config_data={"process_id": "p1", "atom_id": "a1"},
    )

    assert result["_success"] is False
    assert result["error_code"] == ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
    assert result["retryable"] is False
    assert result["exception_type"] == "ApiError"
    assert "attach_process_environment" in result["remediation"]
    assert "manage_runtimes(action='attach')" in result["remediation"]
    _assert_deprecation_metadata(result, "attach_process_atom")
    assert "attach_process_environment" in result["deprecation"]["replacement_actions"]
    assert "manage_runtimes(action='attach')" in result["deprecation"]["replacement_actions"]


def test_attach_component_atom_env_account_fails_closed():
    sdk = MagicMock()
    sdk.component_atom_attachment.create_component_atom_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="attach_component_atom",
        config_data={"component_id": "c1", "atom_id": "a1"},
    )

    assert result["_success"] is False
    assert result["error_code"] == ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
    assert result["retryable"] is False
    assert "attach_component_environment" in result["remediation"]
    _assert_deprecation_metadata(result, "attach_component_atom")
    assert "attach_component_environment" in result["deprecation"]["replacement_actions"]


def test_attach_env_error_text_still_carries_environment_signal():
    """orchestrate_deploy's leg-3 handling re-detects the env signal from the propagated
    error text and records the leg as not_required — the hardened error must keep it."""
    sdk = MagicMock()
    sdk.process_atom_attachment.create_process_atom_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="attach_process_atom",
        config_data={"process_id": "p1", "atom_id": "a1"},
    )

    assert result["error"].startswith("Action 'attach_process_atom' failed: ")
    assert is_environment_account_signal(result["error"]) is True


def test_list_process_atom_attachments_env_account_rejection_is_hardened():
    """Env-enabled accounts reject the list query itself (live-verified, QA bug #138) —
    the error envelope must carry the structured code, remediation, and metadata."""
    sdk = MagicMock()
    sdk.process_atom_attachment.query_process_atom_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_atom_attachments",
    )

    assert result["_success"] is False
    assert result["error"].startswith("Action 'list_process_atom_attachments' failed: ")
    assert result["error_code"] == ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
    assert result["retryable"] is False
    assert result["exception_type"] == "ApiError"
    assert "list_process_environment_attachments" in result["remediation"]
    assert "manage_runtimes(action='list_attachments')" in result["remediation"]
    _assert_deprecation_metadata(result, "list_process_atom_attachments")


def test_list_component_atom_attachments_env_account_rejection_is_hardened():
    sdk = MagicMock()
    sdk.component_atom_attachment.query_component_atom_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="list_component_atom_attachments",
    )

    assert result["_success"] is False
    assert result["error_code"] == ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
    assert "list_component_environment_attachments" in result["remediation"]
    _assert_deprecation_metadata(result, "list_component_atom_attachments")


def test_detach_process_atom_env_account_rejection_is_hardened():
    sdk = MagicMock()
    sdk.process_atom_attachment.delete_process_atom_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="detach_process_atom",
        config_data={"resource_id": "att-1"},
    )

    assert result["_success"] is False
    assert result["error_code"] == ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED
    assert "detach_process_environment" in result["remediation"]
    _assert_deprecation_metadata(result, "detach_process_atom")


def test_atom_action_generic_exception_carries_metadata():
    sdk = MagicMock()
    sdk.process_atom_attachment.create_process_atom_attachment.side_effect = ValueError("bad")

    result = manage_deployment_action(
        sdk, profile="dev", action="attach_process_atom",
        config_data={"process_id": "p1", "atom_id": "a1"},
    )

    assert result["_success"] is False
    assert result["exception_type"] == "ValueError"
    assert "error_code" not in result  # not an env-account rejection
    _assert_deprecation_metadata(result, "attach_process_atom")


def test_non_atom_action_api_error_stays_bare():
    sdk = MagicMock()
    sdk.deployment.query_process_environment_attachment.side_effect = ApiError(
        message=ENV_ACCOUNT_MESSAGE
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_environment_attachments",
    )

    assert result["_success"] is False
    assert result["error"] == f"Action 'list_process_environment_attachments' failed: {ENV_ACCOUNT_MESSAGE}"
    assert result["exception_type"] == "ApiError"
    assert "error_code" not in result
    assert "remediation" not in result
    assert "deprecated" not in result
    assert "deprecation" not in result


def test_attach_non_env_api_error_keeps_router_shape():
    sdk = MagicMock()
    sdk.process_atom_attachment.create_process_atom_attachment.side_effect = ApiError(
        message="boom"
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="attach_process_atom",
        config_data={"process_id": "p1", "atom_id": "a1"},
    )

    assert result["_success"] is False
    assert result["error"] == "Action 'attach_process_atom' failed: boom"
    assert result["exception_type"] == "ApiError"
    assert result.get("error_code") != ENVIRONMENT_ACCOUNT_ATOM_ATTACHMENT_UNSUPPORTED


def test_attach_success_carries_deprecation_metadata():
    sdk = MagicMock()
    sdk.process_atom_attachment.create_process_atom_attachment.return_value = SimpleNamespace(
        id_="att-1", process_id="p1", atom_id="a1"
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="attach_process_atom",
        config_data={"process_id": "p1", "atom_id": "a1"},
    )

    assert result["_success"] is True
    assert result["attachment"]["id"] == "att-1"
    _assert_deprecation_metadata(result, "attach_process_atom")


# ---------------------------------------------------------------------------
# List: empty results warn instead of silently implying no bindings
# ---------------------------------------------------------------------------

def _empty_page():
    return SimpleNamespace(result=[], query_token=None)


def test_list_component_atom_attachments_empty_warns():
    sdk = MagicMock()
    sdk.component_atom_attachment.query_component_atom_attachment.return_value = _empty_page()

    result = manage_deployment_action(
        sdk, profile="dev", action="list_component_atom_attachments",
    )

    assert result["_success"] is True
    assert result["total_count"] == 0
    _assert_deprecation_metadata(result, "list_component_atom_attachments")
    assert "environment-enabled" in result["warning"]
    assert "list_component_environment_attachments" in result["hint"]
    assert "manage_runtimes(action='list_attachments')" in result["hint"]


def test_list_process_atom_attachments_empty_warns():
    sdk = MagicMock()
    sdk.process_atom_attachment.query_process_atom_attachment.return_value = _empty_page()

    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_atom_attachments",
    )

    assert result["_success"] is True
    assert result["total_count"] == 0
    _assert_deprecation_metadata(result, "list_process_atom_attachments")
    assert "environment-enabled" in result["warning"]
    assert "list_process_environment_attachments" in result["hint"]
    assert "manage_runtimes(action='list_attachments')" in result["hint"]


def test_list_process_atom_attachments_non_empty_has_no_warning():
    sdk = MagicMock()
    sdk.process_atom_attachment.query_process_atom_attachment.return_value = SimpleNamespace(
        result=[SimpleNamespace(id="att-1", process_id="p1", atom_id="a1")],
        query_token=None,
    )

    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_atom_attachments",
    )

    assert result["_success"] is True
    assert result["total_count"] == 1
    assert "warning" not in result
    assert "hint" not in result
    _assert_deprecation_metadata(result, "list_process_atom_attachments")


# ---------------------------------------------------------------------------
# Detach: successful delete behavior preserved, metadata added
# ---------------------------------------------------------------------------

def test_detach_component_atom_preserves_delete_and_adds_metadata():
    sdk = MagicMock()

    result = manage_deployment_action(
        sdk, profile="dev", action="detach_component_atom",
        config_data={"resource_id": "att-9"},
    )

    sdk.component_atom_attachment.delete_component_atom_attachment.assert_called_once_with(
        id_="att-9"
    )
    assert result["_success"] is True
    assert result["deleted_id"] == "att-9"
    assert result["message"] == "Component-atom attachment deleted."
    _assert_deprecation_metadata(result, "detach_component_atom")


def test_detach_process_atom_preserves_delete_and_adds_metadata():
    sdk = MagicMock()

    result = manage_deployment_action(
        sdk, profile="dev", action="detach_process_atom",
        config_data={"resource_id": "att-7"},
    )

    sdk.process_atom_attachment.delete_process_atom_attachment.assert_called_once_with(
        id_="att-7"
    )
    assert result["_success"] is True
    assert result["deleted_id"] == "att-7"
    assert result["message"] == "Process-atom attachment deleted."
    _assert_deprecation_metadata(result, "detach_process_atom")


# ---------------------------------------------------------------------------
# Missing parameters: existing error text preserved, error_code + metadata added
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("action,config,expected_error", [
    ("attach_component_atom", {"atom_id": "a1"},
     "component_id is required for 'attach_component_atom'"),
    ("attach_component_atom", {"component_id": "c1"},
     "atom_id is required for 'attach_component_atom'"),
    ("attach_process_atom", {"atom_id": "a1"},
     "process_id is required for 'attach_process_atom'"),
    ("attach_process_atom", {"process_id": "p1"},
     "atom_id is required for 'attach_process_atom'"),
    ("detach_component_atom", {},
     "resource_id is required for 'detach_component_atom'"),
    ("detach_process_atom", {},
     "resource_id is required for 'detach_process_atom'"),
])
def test_missing_params_add_error_code_and_metadata(action, config, expected_error):
    sdk = MagicMock()

    result = manage_deployment_action(sdk, profile="dev", action=action, config_data=config)

    assert result["_success"] is False
    assert result["error"] == expected_error
    assert result["error_code"] == DEPRECATED_ATOM_ATTACHMENT_ACTION
    _assert_deprecation_metadata(result, action)


# ---------------------------------------------------------------------------
# Non-atom actions stay metadata-free
# ---------------------------------------------------------------------------

def test_non_atom_actions_carry_no_deprecation_metadata():
    assert atom_attachment_deprecation_metadata("attach_process_environment") == {}
    assert atom_attachment_deprecation_metadata("deploy") == {}

    sdk = MagicMock()
    sdk.deployment.query_process_environment_attachment.return_value = _empty_page()
    result = manage_deployment_action(
        sdk, profile="dev", action="list_process_environment_attachments",
    )
    assert result["_success"] is True
    assert "deprecated" not in result
    assert "deprecation" not in result


# ---------------------------------------------------------------------------
# manage_runtimes attach/list_attachments confirmed correct (no change)
# ---------------------------------------------------------------------------

def test_manage_runtimes_attach_uses_environment_atom_attachment():
    sdk = MagicMock()
    sdk.environment_atom_attachment.create_environment_atom_attachment.return_value = (
        SimpleNamespace(id="att-1", atom_id="rt-1", environment_id="env-1")
    )

    result = manage_runtimes_action(
        sdk, profile="dev", action="attach",
        resource_id="rt-1", environment_id="env-1",
    )

    assert result["_success"] is True
    call_args = sdk.environment_atom_attachment.create_environment_atom_attachment.call_args
    request = call_args.args[0]
    assert isinstance(request, EnvironmentAtomAttachment)
    assert request.atom_id == "rt-1"
    assert request.environment_id == "env-1"


def test_manage_runtimes_list_attachments_unchanged():
    sdk = MagicMock()
    sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
        SimpleNamespace(
            result=[SimpleNamespace(id="att-1", atom_id="rt-1", environment_id="env-1")],
            query_token=None,
        )
    )

    result = manage_runtimes_action(
        sdk, profile="dev", action="list_attachments", environment_id="env-1",
    )

    assert result["_success"] is True
    assert result["total_count"] == 1
    assert "deprecated" not in result
