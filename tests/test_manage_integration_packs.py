"""Unit tests for manage_integration_packs category module."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from boomi_mcp.categories.integration_packs import manage_integration_packs_action


# ── Helpers ──────────────────────────────────────────────────────────


def _mock_sdk():
    """Return a fully-mocked Boomi SDK with integration pack services."""
    sdk = MagicMock()
    return sdk


def _mock_pack(pack_id="pack-001", name="Test Pack", description="A test pack"):
    obj = MagicMock()
    obj.id_ = pack_id
    obj.name = name
    obj.description = description
    obj.installation_type = "SINGLE"
    return obj


def _mock_publisher_pack(pack_id="pub-001", name="Publisher Pack", description="Pub desc"):
    obj = MagicMock()
    obj.id_ = pack_id
    obj.name = name
    obj.description = description
    obj.installation_type = "SINGLE"
    obj.publisher_packaged_components = None
    return obj


def _mock_instance(instance_id="inst-001", pack_id="pack-001"):
    obj = MagicMock()
    obj.id_ = instance_id
    obj.integration_pack_id = pack_id
    obj.integration_pack_override_name = None
    obj.process_id = []
    return obj


def _mock_release(request_id="req-001", pack_id="pack-001"):
    obj = MagicMock()
    obj.request_id = request_id
    obj.id_ = pack_id
    obj.release_schedule = "IMMEDIATELY"
    obj.release_status_url = "https://api.boomi.com/api/rest/v1/acct/ReleaseIntegrationPackStatus/req-001"
    obj.name = "Test Pack"
    return obj


def _mock_release_status(request_id="req-001", status="SUCCESS"):
    obj = MagicMock()
    obj.request_id = request_id
    obj.release_status = status
    obj.integration_pack_id = "pack-001"
    obj.response_status_code = 200
    obj.name = "Test Pack"
    return obj


def _mock_atom_attachment(attach_id="aa-001", atom_id="atom-001", instance_id="inst-001"):
    obj = MagicMock()
    obj.id_ = attach_id
    obj.atom_id = atom_id
    obj.integration_pack_instance_id = instance_id
    return obj


def _mock_env_attachment(attach_id="ea-001", env_id="env-001", instance_id="inst-001"):
    obj = MagicMock()
    obj.id_ = attach_id
    obj.environment_id = env_id
    obj.integration_pack_instance_id = instance_id
    return obj


def _make_query_result(items, query_token=None):
    result = MagicMock()
    result.result = items
    result.query_token = query_token
    return result


# ── Unknown action ───────────────────────────────────────────────────


class TestUnknownAction:
    def test_unknown_action_returns_error(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "nonexistent")
        assert result["_success"] is False
        assert "Unknown action" in result["error"]
        assert "valid_actions" in result


# ── list_packs ───────────────────────────────────────────────────────


class TestListPacks:
    def test_list_packs_no_filter(self):
        sdk = _mock_sdk()
        sdk.integration_pack.query_integration_pack.return_value = _make_query_result(
            [_mock_pack(), _mock_pack(pack_id="pack-002")]
        )

        result = manage_integration_packs_action(sdk, "dev", "list_packs")

        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.integration_pack.query_integration_pack.assert_called_once_with(request_body=None)

    def test_list_packs_with_name_filter(self):
        sdk = _mock_sdk()
        sdk.integration_pack.query_integration_pack.return_value = _make_query_result(
            [_mock_pack(name="Matching Pack")]
        )

        result = manage_integration_packs_action(sdk, "dev", "list_packs", config_data={"name": "Matching"})

        assert result["_success"] is True
        assert result["total_count"] == 1
        call_args = sdk.integration_pack.query_integration_pack.call_args
        query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert query_config is not None  # Should have a filter

    def test_list_packs_pagination(self):
        sdk = _mock_sdk()
        page1 = _make_query_result([_mock_pack()], query_token="token-page2")
        page2 = _make_query_result([_mock_pack(pack_id="pack-002")])
        sdk.integration_pack.query_integration_pack.return_value = page1
        sdk.integration_pack.query_more_integration_pack.return_value = page2

        result = manage_integration_packs_action(sdk, "dev", "list_packs")

        assert result["_success"] is True
        assert result["total_count"] == 2
        sdk.integration_pack.query_more_integration_pack.assert_called_once_with(
            request_body="token-page2"
        )


# ── get_pack ─────────────────────────────────────────────────────────


class TestGetPack:
    def test_get_pack_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack.get_integration_pack.return_value = _mock_pack()

        result = manage_integration_packs_action(sdk, "dev", "get_pack", resource_id="pack-001")

        assert result["_success"] is True
        assert "pack" in result
        sdk.integration_pack.get_integration_pack.assert_called_once_with(id_="pack-001")

    def test_get_pack_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "get_pack")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── create_publisher_pack ────────────────────────────────────────────


class TestCreatePublisherPack:
    def test_create_publisher_pack_success(self):
        sdk = _mock_sdk()
        sdk.publisher_integration_pack.create_publisher_integration_pack.return_value = (
            _mock_publisher_pack()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "create_publisher_pack",
            config_data={"name": "New Pack", "description": "New Description"},
        )

        assert result["_success"] is True
        assert "publisher_pack" in result
        sdk.publisher_integration_pack.create_publisher_integration_pack.assert_called_once()

    def test_create_publisher_pack_missing_name(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "create_publisher_pack",
            config_data={"description": "Desc only"},
        )
        assert result["_success"] is False
        assert "name" in result["error"]

    def test_create_publisher_pack_missing_description(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "create_publisher_pack",
            config_data={"name": "Name only"},
        )
        assert result["_success"] is False
        assert "description" in result["error"]


# ── delete_publisher_pack ────────────────────────────────────────────


class TestDeletePublisherPack:
    def test_delete_publisher_pack_success(self):
        sdk = _mock_sdk()
        sdk.publisher_integration_pack.delete_publisher_integration_pack.return_value = None

        result = manage_integration_packs_action(
            sdk, "dev", "delete_publisher_pack", resource_id="pub-001",
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "pub-001"
        sdk.publisher_integration_pack.delete_publisher_integration_pack.assert_called_once_with(
            id_="pub-001"
        )

    def test_delete_publisher_pack_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "delete_publisher_pack")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── install_instance ─────────────────────────────────────────────────


class TestInstallInstance:
    def test_install_instance_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack_instance.create_integration_pack_instance.return_value = (
            _mock_instance()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "install_instance",
            config_data={"integration_pack_id": "pack-001"},
        )

        assert result["_success"] is True
        assert "instance" in result
        sdk.integration_pack_instance.create_integration_pack_instance.assert_called_once()

    def test_install_instance_missing_pack_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "install_instance")
        assert result["_success"] is False
        assert "integration_pack_id" in result["error"]

    def test_install_instance_with_override_name(self):
        sdk = _mock_sdk()
        sdk.integration_pack_instance.create_integration_pack_instance.return_value = (
            _mock_instance()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "install_instance",
            config_data={
                "integration_pack_id": "pack-001",
                "integration_pack_override_name": "My Instance",
            },
        )

        assert result["_success"] is True


# ── uninstall_instance ───────────────────────────────────────────────


class TestUninstallInstance:
    def test_uninstall_instance_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack_instance.delete_integration_pack_instance.return_value = None

        result = manage_integration_packs_action(
            sdk, "dev", "uninstall_instance", resource_id="inst-001",
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "inst-001"
        sdk.integration_pack_instance.delete_integration_pack_instance.assert_called_once_with(
            id_="inst-001"
        )

    def test_uninstall_instance_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "uninstall_instance")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── release_pack ─────────────────────────────────────────────────────


class TestReleasePack:
    def test_release_pack_success(self):
        sdk = _mock_sdk()
        sdk.release_integration_pack.create_release_integration_pack.return_value = (
            _mock_release()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "release_pack",
            config_data={"integration_pack_id": "pack-001"},
        )

        assert result["_success"] is True
        assert "release" in result
        sdk.release_integration_pack.create_release_integration_pack.assert_called_once()

    def test_release_pack_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "release_pack")
        assert result["_success"] is False
        assert "integration_pack_id" in result["error"]

    def test_release_pack_scheduled(self):
        sdk = _mock_sdk()
        sdk.release_integration_pack.create_release_integration_pack.return_value = (
            _mock_release()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "release_pack",
            config_data={
                "integration_pack_id": "pack-001",
                "release_schedule": "RELEASE_ON_SPECIFIED_DATE",
                "release_on_date": "2026-06-01",
            },
        )

        assert result["_success"] is True


# ── get_release_status ───────────────────────────────────────────────


class TestGetReleaseStatus:
    def test_get_release_status_success(self):
        sdk = _mock_sdk()
        sdk.release_integration_pack_status.get_release_integration_pack_status.return_value = (
            _mock_release_status()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "get_release_status", resource_id="req-001",
        )

        assert result["_success"] is True
        assert "release_status" in result
        sdk.release_integration_pack_status.get_release_integration_pack_status.assert_called_once_with(
            id_="req-001"
        )

    def test_get_release_status_by_request_id(self):
        sdk = _mock_sdk()
        sdk.release_integration_pack_status.get_release_integration_pack_status.return_value = (
            _mock_release_status()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "get_release_status",
            config_data={"request_id": "req-002"},
        )

        assert result["_success"] is True
        sdk.release_integration_pack_status.get_release_integration_pack_status.assert_called_once_with(
            id_="req-002"
        )

    def test_get_release_status_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "get_release_status")
        assert result["_success"] is False


# ── attach_atom / detach_atom ────────────────────────────────────────


class TestAtomAttachments:
    def test_attach_atom_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack_atom_attachment.create_integration_pack_atom_attachment.return_value = (
            _mock_atom_attachment()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "attach_atom",
            config_data={
                "integration_pack_instance_id": "inst-001",
                "atom_id": "atom-001",
            },
        )

        assert result["_success"] is True
        assert "atom_attachment" in result
        sdk.integration_pack_atom_attachment.create_integration_pack_atom_attachment.assert_called_once()

    def test_attach_atom_missing_instance_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "attach_atom",
            config_data={"atom_id": "atom-001"},
        )
        assert result["_success"] is False
        assert "integration_pack_instance_id" in result["error"]

    def test_attach_atom_missing_atom_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "attach_atom",
            config_data={"integration_pack_instance_id": "inst-001"},
        )
        assert result["_success"] is False
        assert "atom_id" in result["error"]

    def test_detach_atom_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack_atom_attachment.delete_integration_pack_atom_attachment.return_value = None

        result = manage_integration_packs_action(
            sdk, "dev", "detach_atom", resource_id="aa-001",
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "aa-001"
        sdk.integration_pack_atom_attachment.delete_integration_pack_atom_attachment.assert_called_once_with(
            id_="aa-001"
        )

    def test_detach_atom_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "detach_atom")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_list_atom_attachments(self):
        sdk = _mock_sdk()
        sdk.integration_pack_atom_attachment.query_integration_pack_atom_attachment.return_value = (
            _make_query_result([_mock_atom_attachment()])
        )

        result = manage_integration_packs_action(sdk, "dev", "list_atom_attachments")

        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_list_atom_attachments_by_instance_id(self):
        sdk = _mock_sdk()
        sdk.integration_pack_atom_attachment.query_integration_pack_atom_attachment.return_value = (
            _make_query_result([_mock_atom_attachment()])
        )

        result = manage_integration_packs_action(
            sdk, "dev", "list_atom_attachments",
            config_data={"integration_pack_instance_id": "inst-001"},
        )

        assert result["_success"] is True
        call_args = sdk.integration_pack_atom_attachment.query_integration_pack_atom_attachment.call_args
        query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert query_config is not None


# ── attach_environment / detach_environment ──────────────────────────


class TestEnvironmentAttachments:
    def test_attach_environment_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack_environment_attachment.create_integration_pack_environment_attachment.return_value = (
            _mock_env_attachment()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "attach_environment",
            config_data={
                "integration_pack_instance_id": "inst-001",
                "environment_id": "env-001",
            },
        )

        assert result["_success"] is True
        assert "environment_attachment" in result

    def test_attach_environment_missing_instance_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "attach_environment",
            config_data={"environment_id": "env-001"},
        )
        assert result["_success"] is False
        assert "integration_pack_instance_id" in result["error"]

    def test_attach_environment_missing_env_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "attach_environment",
            config_data={"integration_pack_instance_id": "inst-001"},
        )
        assert result["_success"] is False
        assert "environment_id" in result["error"]

    def test_detach_environment_success(self):
        sdk = _mock_sdk()
        sdk.integration_pack_environment_attachment.delete_integration_pack_environment_attachment.return_value = None

        result = manage_integration_packs_action(
            sdk, "dev", "detach_environment", resource_id="ea-001",
        )

        assert result["_success"] is True
        assert result["deleted_id"] == "ea-001"

    def test_detach_environment_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "detach_environment")
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_list_environment_attachments(self):
        sdk = _mock_sdk()
        sdk.integration_pack_environment_attachment.query_integration_pack_environment_attachment.return_value = (
            _make_query_result([_mock_env_attachment()])
        )

        result = manage_integration_packs_action(sdk, "dev", "list_environment_attachments")

        assert result["_success"] is True
        assert result["total_count"] == 1


# ── ApiError handling ────────────────────────────────────────────────


class TestApiErrorHandling:
    def test_api_error_is_caught(self):
        from boomi.net.transport.api_error import ApiError

        sdk = _mock_sdk()
        sdk.integration_pack.get_integration_pack.side_effect = ApiError(
            "Not found", 404, {}
        )

        result = manage_integration_packs_action(
            sdk, "dev", "get_pack", resource_id="nonexistent",
        )

        assert result["_success"] is False
        assert "ApiError" == result["exception_type"]

    def test_generic_exception_is_caught(self):
        sdk = _mock_sdk()
        sdk.integration_pack.get_integration_pack.side_effect = RuntimeError("boom")

        result = manage_integration_packs_action(
            sdk, "dev", "get_pack", resource_id="pack-001",
        )

        assert result["_success"] is False
        assert "boom" in result["error"]
        assert result["exception_type"] == "RuntimeError"


# ── update_publisher_pack ────────────────────────────────────────────


class TestUpdatePublisherPack:
    def test_update_publisher_pack_success(self):
        sdk = _mock_sdk()
        sdk.publisher_integration_pack.update_publisher_integration_pack.return_value = (
            _mock_publisher_pack()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "update_publisher_pack",
            resource_id="pub-001",
            config_data={"description": "Updated description"},
        )

        assert result["_success"] is True
        sdk.publisher_integration_pack.update_publisher_integration_pack.assert_called_once()
        call_kwargs = sdk.publisher_integration_pack.update_publisher_integration_pack.call_args
        assert call_kwargs.kwargs.get("id_") == "pub-001" or call_kwargs[1].get("id_") == "pub-001"

    def test_update_publisher_pack_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "update_publisher_pack",
            config_data={"description": "Desc"},
        )
        assert result["_success"] is False
        assert "resource_id" in result["error"]

    def test_update_publisher_pack_missing_description(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(
            sdk, "dev", "update_publisher_pack",
            resource_id="pub-001",
        )
        assert result["_success"] is False
        assert "description" in result["error"]


# ── update_release ───────────────────────────────────────────────────


class TestUpdateRelease:
    def test_update_release_success(self):
        sdk = _mock_sdk()
        sdk.release_integration_pack.update_release_integration_pack.return_value = (
            _mock_release()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "update_release",
            resource_id="pack-001",
            config_data={
                "release_schedule": "RELEASE_ON_SPECIFIED_DATE",
                "release_on_date": "2026-07-01",
            },
        )

        assert result["_success"] is True
        sdk.release_integration_pack.update_release_integration_pack.assert_called_once()

    def test_update_release_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "update_release")
        assert result["_success"] is False
        assert "resource_id" in result["error"]


# ── list_publisher_packs / get_publisher_pack ────────────────────────


class TestPublisherPackQueries:
    def test_list_publisher_packs(self):
        sdk = _mock_sdk()
        sdk.publisher_integration_pack.query_publisher_integration_pack.return_value = (
            _make_query_result([_mock_publisher_pack()])
        )

        result = manage_integration_packs_action(sdk, "dev", "list_publisher_packs")

        assert result["_success"] is True
        assert result["total_count"] == 1

    def test_get_publisher_pack(self):
        sdk = _mock_sdk()
        sdk.publisher_integration_pack.get_publisher_integration_pack.return_value = (
            _mock_publisher_pack()
        )

        result = manage_integration_packs_action(
            sdk, "dev", "get_publisher_pack", resource_id="pub-001",
        )

        assert result["_success"] is True
        assert "publisher_pack" in result

    def test_get_publisher_pack_missing_id(self):
        sdk = _mock_sdk()
        result = manage_integration_packs_action(sdk, "dev", "get_publisher_pack")
        assert result["_success"] is False


# ── list_instances ───────────────────────────────────────────────────


class TestListInstances:
    def test_list_instances_no_filter(self):
        sdk = _mock_sdk()
        sdk.integration_pack_instance.query_integration_pack_instance.return_value = (
            _make_query_result([_mock_instance()])
        )

        result = manage_integration_packs_action(sdk, "dev", "list_instances")

        assert result["_success"] is True
        assert result["total_count"] == 1
        sdk.integration_pack_instance.query_integration_pack_instance.assert_called_once_with(
            request_body=None
        )

    def test_list_instances_with_pack_id_filter(self):
        sdk = _mock_sdk()
        sdk.integration_pack_instance.query_integration_pack_instance.return_value = (
            _make_query_result([_mock_instance()])
        )

        result = manage_integration_packs_action(
            sdk, "dev", "list_instances",
            config_data={"integration_pack_id": "pack-001"},
        )

        assert result["_success"] is True
        call_args = sdk.integration_pack_instance.query_integration_pack_instance.call_args
        query_config = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert query_config is not None
