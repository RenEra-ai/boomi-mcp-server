"""Unit tests for manage_runtimes list filters and detach validation (mocked SDK)."""

import sys
import os
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.boomi_mcp.categories.runtimes import (
    _action_list,
    _action_detach,
    _match_name_pattern,
)


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_sdk():
    sdk = MagicMock()
    return sdk


def _make_runtime(id_="rt-1", name="Test Runtime", type_="ATOM", status="ONLINE"):
    """Create a mock SDK Atom object."""
    rt = MagicMock()
    rt.id_ = id_
    rt.name = name
    rt.type_ = MagicMock(value=type_)
    rt.status = MagicMock(value=status)
    rt.host_name = None
    rt.current_version = None
    rt.date_installed = None
    rt.created_by = None
    rt.cloud_id = None
    rt.cloud_name = None
    rt.cloud_molecule_id = None
    rt.cloud_molecule_name = None
    rt.cloud_owner_name = None
    rt.instance_id = None
    rt.status_detail = None
    rt.is_cloud_attachment = None
    rt.purge_history_days = None
    rt.purge_immediate = None
    rt.force_restart_time = None
    rt.capabilities = None
    return rt


def _make_query_result(items, query_token=None):
    """Create a mock query result with .result and .query_token."""
    result = MagicMock()
    result.result = items
    result.query_token = query_token
    return result


def _make_attachment(id_="att-1", atom_id="rt-1", environment_id="env-1"):
    """Create a mock EnvironmentAtomAttachment object."""
    att = MagicMock()
    att.id_ = id_
    att.atom_id = atom_id
    att.environment_id = environment_id
    return att


# ── TestMatchNamePattern ─────────────────────────────────────────────


class TestMatchNamePattern:
    """Unit tests for the _match_name_pattern helper."""

    def test_bare_text_matches_substring(self):
        assert _match_name_pattern("Production Atom", "Prod") is True

    def test_bare_text_no_match(self):
        assert _match_name_pattern("Production Atom", "Staging") is False

    def test_bare_text_case_sensitive(self):
        assert _match_name_pattern("Production Atom", "prod") is False

    def test_prefix_pattern(self):
        assert _match_name_pattern("Production Atom", "Prod%") is True
        assert _match_name_pattern("My Prod Atom", "Prod%") is False

    def test_suffix_pattern(self):
        assert _match_name_pattern("Production Atom", "%Atom") is True
        assert _match_name_pattern("Atom Server", "%Atom") is False

    def test_contains_pattern(self):
        assert _match_name_pattern("Production Atom", "%duct%") is True
        assert _match_name_pattern("Dev Atom", "%duct%") is False

    def test_wildcard_only(self):
        assert _match_name_pattern("Anything", "%") is True
        assert _match_name_pattern("", "%") is True

    def test_empty_pattern(self):
        assert _match_name_pattern("Anything", "") is True

    def test_explicit_percent_preserved(self):
        # %Prod% should match same as substring, not double-wrap
        assert _match_name_pattern("Production Atom", "%Prod%") is True
        assert _match_name_pattern("My Prod Server", "%Prod%") is True
        assert _match_name_pattern("Dev Atom", "%Prod%") is False


# ── TestActionListNameExact (QA-012) ─────────────────────────────────


class TestActionListNameExact:
    """Test config.name exact-match filter for list action."""

    def test_name_exact_match(self):
        sdk = _make_sdk()
        rt = _make_runtime(id_="rt-1", name="Production Atom")
        sdk.atom.query_atom.return_value = _make_query_result([rt])

        result = _action_list(sdk, "dev", name="Production Atom")

        assert result["_success"] is True
        assert result["total_count"] == 1
        assert result["runtimes"][0]["name"] == "Production Atom"
        # Verify SDK was called with EQUALS expression
        call_args = sdk.atom.query_atom.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        expr = qc.query_filter.expression
        assert expr.argument == ["Production Atom"]

    def test_name_exact_no_match(self):
        sdk = _make_sdk()
        sdk.atom.query_atom.return_value = _make_query_result([])

        result = _action_list(sdk, "dev", name="Nonexistent")

        assert result["_success"] is True
        assert result["total_count"] == 0
        assert result["runtimes"] == []

    def test_name_precedence_over_name_pattern(self):
        sdk = _make_sdk()
        rt = _make_runtime(id_="rt-1", name="Exact Name")
        sdk.atom.query_atom.return_value = _make_query_result([rt])

        result = _action_list(sdk, "dev", name="Exact Name", name_pattern="Pattern")

        assert result["_success"] is True
        assert result["total_count"] == 1
        # name_pattern should not have filtered further
        assert result["runtimes"][0]["name"] == "Exact Name"


# ── TestActionListNamePattern (QA-011) ───────────────────────────────


class TestActionListNamePattern:
    """Test config.name_pattern wrapper-side filtering for list action."""

    def _setup_runtimes(self, sdk):
        """Set up SDK with three runtimes for pattern tests."""
        runtimes = [
            _make_runtime(id_="rt-1", name="Prod Atom"),
            _make_runtime(id_="rt-2", name="Dev Atom"),
            _make_runtime(id_="rt-3", name="Prod Molecule"),
        ]
        sdk.atom.query_atom.return_value = _make_query_result(runtimes)
        return runtimes

    def test_bare_text_filters_locally(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="Prod")

        assert result["_success"] is True
        assert result["total_count"] == 2
        names = [r["name"] for r in result["runtimes"]]
        assert "Prod Atom" in names
        assert "Prod Molecule" in names
        assert "Dev Atom" not in names
        # SDK should have been called with no expression (fetching all)
        call_args = sdk.atom.query_atom.call_args
        qc = call_args.kwargs.get("request_body") or call_args[1].get("request_body")
        assert not hasattr(qc, 'query_filter') or qc.query_filter is None

    def test_prefix_pattern(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="Prod%")

        assert result["_success"] is True
        assert result["total_count"] == 2
        names = [r["name"] for r in result["runtimes"]]
        assert "Prod Atom" in names
        assert "Prod Molecule" in names

    def test_suffix_pattern(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="%Atom")

        assert result["_success"] is True
        assert result["total_count"] == 2
        names = [r["name"] for r in result["runtimes"]]
        assert "Prod Atom" in names
        assert "Dev Atom" in names

    def test_wildcard_returns_all(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="%")

        assert result["_success"] is True
        assert result["total_count"] == 3

    def test_no_match(self):
        sdk = _make_sdk()
        self._setup_runtimes(sdk)

        result = _action_list(sdk, "dev", name_pattern="Staging")

        assert result["_success"] is True
        assert result["total_count"] == 0


# ── TestActionDetachValidation (QA-013) ──────────────────────────────


class TestActionDetachValidation:
    """Test detach action validation for attachment_id vs runtime_id."""

    def test_detach_with_valid_attachment_id_no_env(self):
        """Direct detach with a known attachment ID should succeed."""
        sdk = _make_sdk()
        att = _make_attachment(id_="att-1", atom_id="rt-1", environment_id="env-1")
        sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
            _make_query_result([att])
        )

        result = _action_detach(sdk, "dev", resource_id="att-1")

        assert result["_success"] is True
        assert result["detached_attachment_id"] == "att-1"
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_called_once_with(
            id_="att-1"
        )

    def test_detach_with_runtime_id_and_env(self):
        """Lookup path: runtime_id + environment_id should find and delete attachment."""
        sdk = _make_sdk()
        att = _make_attachment(id_="att-1", atom_id="rt-1", environment_id="env-1")
        sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
            _make_query_result([att])
        )

        result = _action_detach(sdk, "dev", resource_id="rt-1", environment_id="env-1")

        assert result["_success"] is True
        assert result["detached_attachment_id"] == "att-1"
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_called_once_with(
            id_="att-1"
        )

    def test_detach_unknown_id_no_env_returns_error(self):
        """runtime_id without environment_id should return validation error."""
        sdk = _make_sdk()
        # Return attachments that don't match the resource_id
        att = _make_attachment(id_="att-1", atom_id="rt-1", environment_id="env-1")
        sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
            _make_query_result([att])
        )

        result = _action_detach(sdk, "dev", resource_id="rt-1")

        assert result["_success"] is False
        assert "environment_id is required" in result["error"]
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_not_called()

    def test_detach_missing_resource_id(self):
        sdk = _make_sdk()

        result = _action_detach(sdk, "dev")

        assert result["_success"] is False
        assert "resource_id is required" in result["error"]

    def test_detach_runtime_id_with_env_no_match(self):
        """runtime_id + environment_id but no matching attachment."""
        sdk = _make_sdk()
        att = _make_attachment(id_="att-1", atom_id="rt-other", environment_id="env-1")
        sdk.environment_atom_attachment.query_environment_atom_attachment.return_value = (
            _make_query_result([att])
        )

        result = _action_detach(sdk, "dev", resource_id="rt-1", environment_id="env-1")

        assert result["_success"] is False
        assert "No attachment found" in result["error"]
        sdk.environment_atom_attachment.delete_environment_atom_attachment.assert_not_called()
