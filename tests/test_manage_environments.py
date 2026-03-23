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
    _action_get_map_extension,
    _action_bulk_get_map_extensions,
    _action_list_map_udf_summaries,
    _action_create_map_udf,
    _action_get_map_udf,
    _action_update_map_udf,
    _action_delete_map_udf,
    _action_list_map_external_components,
    _action_list_environment_roles,
    _action_create_environment_role,
    _action_delete_environment_role,
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


# ── _action_get_map_extension ────────────────────────────────────────


class TestActionGetMapExtension:
    def test_returns_map_extension(self):
        sdk = _make_sdk()
        mock_result = MagicMock()
        mock_result.id_ = "me-123"
        mock_result.environment_id = "env-1"
        mock_result.name = "Test Map"
        sdk.environment_map_extension.get_environment_map_extension.return_value = mock_result

        result = _action_get_map_extension(sdk, "dev", resource_id="me-123")
        assert result["_success"] is True
        assert "map_extension" in result
        sdk.environment_map_extension.get_environment_map_extension.assert_called_once_with(id_="me-123")

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_map_extension(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _action_bulk_get_map_extensions ──────────────────────────────────


class TestActionBulkGetMapExtensions:
    def test_returns_bulk_responses(self):
        sdk = _make_sdk()
        item1 = MagicMock()
        item1.id_ = "me-1"
        item1.status_code = 200
        item1.error_message = None
        item1.result = MagicMock()
        item1.result.name = "Map1"
        item2 = MagicMock()
        item2.id_ = "me-2"
        item2.status_code = 200
        item2.error_message = None
        item2.result = MagicMock()
        item2.result.name = "Map2"
        bulk_resp = MagicMock()
        bulk_resp.response = [item1, item2]
        sdk.environment_map_extension.bulk_environment_map_extension.return_value = bulk_resp

        result = _action_bulk_get_map_extensions(sdk, "dev", ids=["me-1", "me-2"])
        assert result["_success"] is True
        assert result["total_count"] == 2
        assert len(result["responses"]) == 2

    def test_requires_ids_list(self):
        sdk = _make_sdk()
        result = _action_bulk_get_map_extensions(sdk, "dev")
        assert result["_success"] is False
        assert "ids" in result["error"]

    def test_requires_ids_to_be_list(self):
        sdk = _make_sdk()
        result = _action_bulk_get_map_extensions(sdk, "dev", ids="not-a-list")
        assert result["_success"] is False
        assert "ids" in result["error"]


# ── _action_list_map_udf_summaries ───────────────────────────────────


class TestActionListMapUdfSummaries:
    def test_queries_by_environment_id(self):
        sdk = _make_sdk()
        query_result = MagicMock()
        query_result.result = [MagicMock(), MagicMock()]
        query_result.query_token = None
        sdk.environment_map_extension_user_defined_function_summary.query_environment_map_extension_user_defined_function_summary.return_value = query_result

        result = _action_list_map_udf_summaries(sdk, "dev", environment_id="env-1")
        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.environment_map_extension_user_defined_function_summary.query_environment_map_extension_user_defined_function_summary.assert_called_once()

    def test_queries_by_extension_group_id(self):
        sdk = _make_sdk()
        query_result = MagicMock()
        query_result.result = [MagicMock()]
        query_result.query_token = None
        sdk.environment_map_extension_user_defined_function_summary.query_environment_map_extension_user_defined_function_summary.return_value = query_result

        result = _action_list_map_udf_summaries(sdk, "dev", extension_group_id="eg-1")
        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_requires_filter(self):
        sdk = _make_sdk()
        result = _action_list_map_udf_summaries(sdk, "dev")
        assert result["_success"] is False
        assert "environment_id" in result["error"]

    def test_paginates(self):
        sdk = _make_sdk()
        page1 = MagicMock()
        page1.result = [MagicMock()]
        page1.query_token = "token-1"
        page2 = MagicMock()
        page2.result = [MagicMock()]
        page2.query_token = None
        sdk.environment_map_extension_user_defined_function_summary.query_environment_map_extension_user_defined_function_summary.return_value = page1
        sdk.environment_map_extension_user_defined_function_summary.query_more_environment_map_extension_user_defined_function_summary.return_value = page2

        result = _action_list_map_udf_summaries(sdk, "dev", environment_id="env-1")
        assert result["_success"] is True
        assert result["total_count"] == 2


# ── _action_create_map_udf ──────────────────────────────────────────


SAMPLE_UDF_DATA = {
    "name": "MyUDF",
    "Inputs": {"Input": [{"name": "in1", "key": 1}]},
    "Outputs": {"Output": [{"name": "out1", "key": 1}]},
    "Steps": {"Step": [{
        "position": 1, "id": "FUNCEXT--1", "type": "MathCeil",
        "Configuration": {},
        "Inputs": {"Input": [{"name": "in1", "key": 1}]},
        "Outputs": {"Output": [{"name": "out1", "key": 1}]},
    }]},
    "Mappings": {"Mapping": [{"fromFunction": "0", "fromKey": 1, "toFunction": "1", "toKey": 1}]},
}


class TestActionCreateMapUdf:
    def test_creates_udf(self):
        sdk = _make_sdk()
        mock_result = MagicMock()
        mock_result.id_ = "udf-new"
        sdk.environment_map_extension_user_defined_function.create_environment_map_extension_user_defined_function.return_value = mock_result

        result = _action_create_map_udf(sdk, "dev", udf_data=SAMPLE_UDF_DATA)
        assert result["_success"] is True
        assert "udf" in result
        sdk.environment_map_extension_user_defined_function.create_environment_map_extension_user_defined_function.assert_called_once()

    def test_requires_udf_data(self):
        sdk = _make_sdk()
        result = _action_create_map_udf(sdk, "dev")
        assert result["_success"] is False
        assert "udf_data" in result["error"]

    def test_requires_udf_data_dict(self):
        sdk = _make_sdk()
        result = _action_create_map_udf(sdk, "dev", udf_data="not-a-dict")
        assert result["_success"] is False
        assert "udf_data" in result["error"]


# ── _action_get_map_udf ─────────────────────────────────────────────


class TestActionGetMapUdf:
    def test_returns_udf(self):
        sdk = _make_sdk()
        mock_result = MagicMock()
        mock_result.id_ = "udf-1"
        sdk.environment_map_extension_user_defined_function.get_environment_map_extension_user_defined_function.return_value = mock_result

        result = _action_get_map_udf(sdk, "dev", resource_id="udf-1")
        assert result["_success"] is True
        assert "udf" in result
        sdk.environment_map_extension_user_defined_function.get_environment_map_extension_user_defined_function.assert_called_once_with(id_="udf-1")

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_get_map_udf(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _action_update_map_udf ──────────────────────────────────────────


class TestActionUpdateMapUdf:
    def test_updates_udf(self):
        sdk = _make_sdk()
        mock_result = MagicMock()
        mock_result.id_ = "udf-1"
        sdk.environment_map_extension_user_defined_function.update_environment_map_extension_user_defined_function.return_value = mock_result

        udf_data = dict(SAMPLE_UDF_DATA)
        udf_data["name"] = "Updated"
        result = _action_update_map_udf(sdk, "dev", resource_id="udf-1", udf_data=udf_data)
        assert result["_success"] is True
        assert "udf" in result
        sdk.environment_map_extension_user_defined_function.update_environment_map_extension_user_defined_function.assert_called_once()

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_update_map_udf(sdk, "dev", udf_data={"name": "X"})
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_requires_udf_data(self):
        sdk = _make_sdk()
        result = _action_update_map_udf(sdk, "dev", resource_id="udf-1")
        assert result["_success"] is False
        assert "udf_data" in result["error"]


# ── _action_delete_map_udf ──────────────────────────────────────────


class TestActionDeleteMapUdf:
    def test_deletes_udf(self):
        sdk = _make_sdk()
        sdk.environment_map_extension_user_defined_function.delete_environment_map_extension_user_defined_function.return_value = None

        result = _action_delete_map_udf(sdk, "dev", resource_id="udf-1")
        assert result["_success"] is True
        assert "udf-1" in result["message"]
        sdk.environment_map_extension_user_defined_function.delete_environment_map_extension_user_defined_function.assert_called_once_with(id_="udf-1")

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_delete_map_udf(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── _action_list_map_external_components ─────────────────────────────


class TestActionListMapExternalComponents:
    def test_queries_by_eme_id(self):
        sdk = _make_sdk()
        query_result = MagicMock()
        query_result.result = [MagicMock(), MagicMock()]
        query_result.query_token = None
        sdk.environment_map_extension_external_component.query_environment_map_extension_external_component.return_value = query_result

        result = _action_list_map_external_components(sdk, "dev", environment_map_extension_id="eme-1")
        assert result["_success"] is True
        assert result["total_count"] == 2

    def test_queries_by_component_id(self):
        sdk = _make_sdk()
        query_result = MagicMock()
        query_result.result = [MagicMock()]
        query_result.query_token = None
        sdk.environment_map_extension_external_component.query_environment_map_extension_external_component.return_value = query_result

        result = _action_list_map_external_components(sdk, "dev", component_id="comp-1")
        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_requires_filter(self):
        sdk = _make_sdk()
        result = _action_list_map_external_components(sdk, "dev")
        assert result["_success"] is False
        assert "environment_map_extension_id" in result["error"]

    def test_paginates(self):
        sdk = _make_sdk()
        page1 = MagicMock()
        page1.result = [MagicMock()]
        page1.query_token = "token-1"
        page2 = MagicMock()
        page2.result = [MagicMock(), MagicMock()]
        page2.query_token = None
        sdk.environment_map_extension_external_component.query_environment_map_extension_external_component.return_value = page1
        sdk.environment_map_extension_external_component.query_more_environment_map_extension_external_component.return_value = page2

        result = _action_list_map_external_components(sdk, "dev", environment_map_extension_id="eme-1")
        assert result["_success"] is True
        assert result["total_count"] == 3


# ── _action_list_environment_roles ───────────────────────────────────


class TestActionListEnvironmentRoles:
    def test_queries_by_environment_id(self):
        sdk = _make_sdk()
        role1 = MagicMock()
        role1.environment_id = "env-1"
        role1.role_id = "role-1"
        role1.id_ = "env-1-role-1"
        query_result = MagicMock()
        query_result.result = [role1]
        query_result.query_token = None
        sdk.environment_role.query_environment_role.return_value = query_result

        result = _action_list_environment_roles(sdk, "dev", environment_id="env-1")
        assert result["_success"] is True
        assert result["total_count"] == 1
        sdk.environment_role.query_environment_role.assert_called_once()

    def test_queries_by_role_id(self):
        sdk = _make_sdk()
        query_result = MagicMock()
        query_result.result = [MagicMock()]
        query_result.query_token = None
        sdk.environment_role.query_environment_role.return_value = query_result

        result = _action_list_environment_roles(sdk, "dev", role_id="role-1")
        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_requires_filter(self):
        sdk = _make_sdk()
        result = _action_list_environment_roles(sdk, "dev")
        assert result["_success"] is False
        assert "environment_id" in result["error"]

    def test_paginates(self):
        sdk = _make_sdk()
        page1 = MagicMock()
        page1.result = [MagicMock()]
        page1.query_token = "token-1"
        page2 = MagicMock()
        page2.result = [MagicMock()]
        page2.query_token = None
        sdk.environment_role.query_environment_role.return_value = page1
        sdk.environment_role.query_more_environment_role.return_value = page2

        result = _action_list_environment_roles(sdk, "dev", environment_id="env-1")
        assert result["_success"] is True
        assert result["total_count"] == 2


# ── _action_create_environment_role ──────────────────────────────────


class TestActionCreateEnvironmentRole:
    def test_creates_role(self):
        sdk = _make_sdk()
        mock_result = MagicMock()
        mock_result.environment_id = "env-1"
        mock_result.role_id = "role-1"
        mock_result.id_ = "env-1-role-1"
        sdk.environment_role.create_environment_role.return_value = mock_result

        result = _action_create_environment_role(sdk, "dev", environment_id="env-1", role_id="role-1")
        assert result["_success"] is True
        assert "environment_role" in result
        sdk.environment_role.create_environment_role.assert_called_once()

    def test_requires_environment_id(self):
        sdk = _make_sdk()
        result = _action_create_environment_role(sdk, "dev", role_id="role-1")
        assert result["_success"] is False
        assert "environment_id" in result["error"]

    def test_requires_role_id(self):
        sdk = _make_sdk()
        result = _action_create_environment_role(sdk, "dev", environment_id="env-1")
        assert result["_success"] is False
        assert "role_id" in result["error"]


# ── _action_delete_environment_role ──────────────────────────────────


class TestActionDeleteEnvironmentRole:
    def test_deletes_role(self):
        sdk = _make_sdk()
        sdk.environment_role.delete_environment_role.return_value = None

        result = _action_delete_environment_role(sdk, "dev", resource_id="env-1-role-1")
        assert result["_success"] is True
        assert "env-1-role-1" in result["message"]
        sdk.environment_role.delete_environment_role.assert_called_once_with(id_="env-1-role-1")

    def test_requires_resource_id(self):
        sdk = _make_sdk()
        result = _action_delete_environment_role(sdk, "dev")
        assert result["_success"] is False
        assert "resource_id" in result["error"]
