"""Unit tests for manage_environments extensions workflow (mocked SDK)."""

import sys
import os
from unittest.mock import MagicMock, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from boomi.models import EnvironmentExtensions

from src.boomi_mcp.categories.environments import (
    _normalize_extensions,
    _parse_extensions_response,
    _deep_merge,
    _merge_lists,
    _verify_extensions_persisted,
    _action_get,
    _action_get_extensions,
    _action_update_extensions,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_sdk():
    sdk = MagicMock()
    return sdk


def _make_extensions_model(data: dict) -> EnvironmentExtensions:
    """Create a real SDK EnvironmentExtensions model from API-style dict."""
    return EnvironmentExtensions._unmap(data)


SAMPLE_EXTENSIONS = {
    "environmentId": "env-123",
    "id": "env-123",
    "partial": False,
    "connections": {
        "connection": [{
            "id": "conn-1",
            "name": "My Database",
            "field": [
                {"id": "host", "value": "", "useDefault": True},
                {"id": "port", "value": "", "useDefault": True},
                {"id": "username", "value": "", "useDefault": True},
                {"id": "password", "value": "", "useDefault": True, "encryptedValueSet": False},
            ],
        }],
    },
    "operations": {
        "operation": [{
            "id": "op-1",
            "name": "My Operation",
            "field": [
                {"id": "timeout", "value": "30000", "useDefault": True},
            ],
        }],
    },
}


# ── _action_get ──────────────────────────────────────────────────────


def _make_env_obj(id_="env-1", name="Test Env", classification="TEST"):
    """Create a mock SDK Environment object."""
    env = MagicMock()
    env.id_ = id_
    env.name = name
    env.classification = MagicMock(value=classification)
    env.created_by = None
    env.created_date = None
    return env


class TestActionGet:
    def test_active_environment(self):
        sdk = _make_sdk()
        env = _make_env_obj()
        sdk.environment.get_environment.return_value = env
        query_result = MagicMock()
        query_result.result = [env]
        sdk.environment.query_environment.return_value = query_result

        result = _action_get(sdk, "dev", resource_id="env-1")
        assert result["_success"] is True
        assert result["environment"]["id"] == "env-1"
        assert "deleted" not in result["environment"]
        assert "warning" not in result
        assert "soft_delete_check" not in result

    def test_soft_deleted_environment(self):
        sdk = _make_sdk()
        env = _make_env_obj()
        sdk.environment.get_environment.return_value = env
        query_result = MagicMock()
        query_result.result = []
        query_result.query_token = None
        sdk.environment.query_environment.return_value = query_result

        result = _action_get(sdk, "dev", resource_id="env-1")
        assert result["_success"] is True
        assert result["environment"]["deleted"] is True
        assert "soft-delete" in result["warning"]

    def test_probe_failure_returns_success_with_unavailable(self):
        sdk = _make_sdk()
        env = _make_env_obj()
        sdk.environment.get_environment.return_value = env
        sdk.environment.query_environment.side_effect = Exception("transient")

        result = _action_get(sdk, "dev", resource_id="env-1")
        assert result["_success"] is True
        assert result["environment"]["id"] == "env-1"
        assert "deleted" not in result["environment"]
        assert result["soft_delete_check"] == "unavailable"

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_get(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _normalize_extensions ────────────────────────────────────────────


class TestNormalizeExtensions:
    def test_returns_api_style_dict_from_sdk_model(self):
        model = _make_extensions_model(SAMPLE_EXTENSIONS)
        result = _normalize_extensions(model)
        assert result["environmentId"] == "env-123"
        assert "connections" in result
        assert "@type" not in result

    def test_strips_at_type_recursively(self):
        model = _make_extensions_model(SAMPLE_EXTENSIONS)
        result = _normalize_extensions(model)
        # Check nested levels have no @type
        conn = result["connections"]["connection"][0]
        assert "@type" not in conn
        field = conn["field"][0]
        assert "@type" not in field

    def test_handles_plain_dict(self):
        result = _normalize_extensions({"environmentId": "e1", "@type": "X"})
        assert result == {"environmentId": "e1"}

    def test_handles_empty_input(self):
        assert _normalize_extensions({}) == {}
        assert _normalize_extensions(None) == {}


# ── _parse_extensions_response ───────────────────────────────────────


class TestParseExtensionsResponse:
    def test_returns_real_data_from_sdk_model(self):
        model = _make_extensions_model(SAMPLE_EXTENSIONS)
        result = _parse_extensions_response(model)
        assert result["environment_id"] == "env-123"
        assert result["connections"]["count"] == 1
        assert len(result["connections"]["items"]) == 1
        assert result["operations"]["count"] == 1

    def test_preserves_usedefault_true_fields(self):
        """Fields with useDefault=true must NOT be filtered out."""
        model = _make_extensions_model(SAMPLE_EXTENSIONS)
        result = _parse_extensions_response(model)
        conn = result["connections"]["items"][0]
        fields = conn["field"]
        assert len(fields) == 4
        host_field = next(f for f in fields if f["id"] == "host")
        assert host_field["useDefault"] is True

    def test_empty_extension_types_have_zero_count(self):
        model = _make_extensions_model({
            "environmentId": "env-1",
            "id": "env-1",
        })
        result = _parse_extensions_response(model)
        assert result["properties"]["count"] == 0
        assert result["trading_partners"]["count"] == 0


# ── _deep_merge / _merge_lists ───────────────────────────────────────


class TestDeepMerge:
    def test_preserves_sibling_extension_types(self):
        base = {
            "connections": {"connection": [{"id": "c1", "field": []}]},
            "operations": {"operation": [{"id": "o1", "field": []}]},
        }
        override = {
            "connections": {"connection": [{"id": "c1", "field": [{"id": "host", "value": "new"}]}]},
        }
        merged = _deep_merge(base, override)
        assert "operations" in merged
        assert merged["operations"]["operation"][0]["id"] == "o1"

    def test_preserves_sibling_fields_in_connection(self):
        base = {
            "connection": [{
                "id": "c1",
                "field": [
                    {"id": "host", "value": "old"},
                    {"id": "port", "value": "1433"},
                ],
            }],
        }
        override = {
            "connection": [{
                "id": "c1",
                "field": [
                    {"id": "host", "value": "new-host"},
                ],
            }],
        }
        merged = _deep_merge(base, override)
        fields = merged["connection"][0]["field"]
        # The merge-by-id should preserve 'port' and update 'host'
        assert len(fields) == 2
        host = next(f for f in fields if f["id"] == "host")
        assert host["value"] == "new-host"
        port = next(f for f in fields if f["id"] == "port")
        assert port["value"] == "1433"


# ── _action_get_extensions ───────────────────────────────────────────


class TestActionGetExtensions:
    def test_returns_extensions_from_sdk_model(self):
        sdk = _make_sdk()
        model = _make_extensions_model(SAMPLE_EXTENSIONS)
        sdk.environment_extensions.get_environment_extensions.return_value = model

        result = _action_get_extensions(sdk, "dev", resource_id="env-123")
        assert result["_success"] is True
        assert result["extensions"]["connections"]["count"] == 1
        assert len(result["extensions"]["connections"]["items"][0]["field"]) == 4

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_extensions(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _action_update_extensions ────────────────────────────────────────


class TestActionUpdateExtensions:
    def test_sets_id_environmentid_and_partial(self):
        sdk = _make_sdk()
        # Mock GET for current extensions
        current_model = _make_extensions_model(SAMPLE_EXTENSIONS)
        # Mock GET for verification (return updated data)
        updated_data = dict(SAMPLE_EXTENSIONS)
        updated_data["connections"] = {
            "connection": [{
                "id": "conn-1",
                "name": "My Database",
                "field": [
                    {"id": "host", "value": "new-host", "useDefault": False},
                    {"id": "port", "value": "", "useDefault": True},
                    {"id": "username", "value": "", "useDefault": True},
                    {"id": "password", "value": "", "useDefault": True},
                ],
            }],
        }
        verify_model = _make_extensions_model(updated_data)
        sdk.environment_extensions.get_environment_extensions.side_effect = [
            current_model, verify_model
        ]
        sdk.environment_extensions.update_environment_extensions.return_value = MagicMock()

        result = _action_update_extensions(
            sdk, "dev",
            resource_id="env-123",
            extensions={
                "connections": {"connection": [{
                    "id": "conn-1",
                    "field": [{"id": "host", "value": "new-host", "useDefault": False}],
                }]},
            },
        )

        assert result["_success"] is True
        # Verify the request body has id, environmentId, and partial
        call_kwargs = sdk.environment_extensions.update_environment_extensions.call_args
        request_body = call_kwargs.kwargs.get("request_body") or call_kwargs[1].get("request_body")
        mapped = request_body._map()
        assert mapped["id"] == "env-123"
        assert mapped["environmentId"] == "env-123"
        assert mapped["partial"] is True

    def test_uses_unmap_for_model_construction(self):
        sdk = _make_sdk()
        current_model = _make_extensions_model(SAMPLE_EXTENSIONS)
        verify_model = _make_extensions_model(SAMPLE_EXTENSIONS)
        sdk.environment_extensions.get_environment_extensions.side_effect = [
            current_model, verify_model
        ]
        sdk.environment_extensions.update_environment_extensions.return_value = MagicMock()

        _action_update_extensions(
            sdk, "dev",
            resource_id="env-123",
            extensions={"connections": {"connection": [{"id": "conn-1", "field": []}]}},
        )

        call_kwargs = sdk.environment_extensions.update_environment_extensions.call_args
        request_body = call_kwargs.kwargs.get("request_body") or call_kwargs[1].get("request_body")
        # Must be an SDK model, not a plain dict
        assert isinstance(request_body, EnvironmentExtensions)
        # Must produce valid API output
        mapped = request_body._map()
        assert "environmentId" in mapped

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_extensions(sdk, "dev", extensions={})
        assert result["_success"] is False

    def test_requires_extensions(self):
        sdk = _make_sdk()
        result = _action_update_extensions(sdk, "dev", resource_id="env-1")
        assert result["_success"] is False


# ── _verify_extensions_persisted ─────────────────────────────────────


class TestVerifyExtensionsPersisted:
    def test_no_warnings_when_values_match(self):
        requested = {
            "connections": {"connection": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "myhost", "useDefault": False}],
            }]},
        }
        verified = {
            "connections": {"count": 1, "items": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "myhost", "useDefault": False}],
            }]},
        }
        warnings = _verify_extensions_persisted(requested, verified)
        assert warnings == []

    def test_warning_when_still_usedefault_true(self):
        requested = {
            "connections": {"connection": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "myhost", "useDefault": False}],
            }]},
        }
        verified = {
            "connections": {"count": 1, "items": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "", "useDefault": True}],
            }]},
        }
        warnings = _verify_extensions_persisted(requested, verified)
        assert len(warnings) == 1
        assert "useDefault=true" in warnings[0]

    def test_encrypted_field_no_warning_when_usedefault_false(self):
        """Encrypted fields return empty value — only check useDefault."""
        requested = {
            "connections": {"connection": [{
                "id": "conn-1",
                "field": [{"id": "password", "value": "secret123", "useDefault": False}],
            }]},
        }
        verified = {
            "connections": {"count": 1, "items": [{
                "id": "conn-1",
                "field": [{"id": "password", "value": "", "useDefault": False}],
            }]},
        }
        warnings = _verify_extensions_persisted(requested, verified)
        assert warnings == []

    def test_value_mismatch_warning(self):
        requested = {
            "connections": {"connection": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "new-host", "useDefault": False}],
            }]},
        }
        verified = {
            "connections": {"count": 1, "items": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "old-host", "useDefault": False}],
            }]},
        }
        warnings = _verify_extensions_persisted(requested, verified)
        assert len(warnings) == 1
        assert "mismatch" in warnings[0]

    def test_requesting_usedefault_true_skips_verification(self):
        requested = {
            "connections": {"connection": [{
                "id": "conn-1",
                "field": [{"id": "host", "useDefault": True}],
            }]},
        }
        verified = {
            "connections": {"count": 1, "items": [{
                "id": "conn-1",
                "field": [{"id": "host", "value": "", "useDefault": True}],
            }]},
        }
        warnings = _verify_extensions_persisted(requested, verified)
        assert warnings == []
