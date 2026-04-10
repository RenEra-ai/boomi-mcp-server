"""Regression tests for BUG-41: _lookup_deployment_id must select the most
recent active deployment when multiple results match a package/environment pair.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.categories.deployment.packages import _lookup_deployment_id


def _make_dep(deployment_id, active, deployed_date):
    return SimpleNamespace(
        deployment_id=deployment_id,
        id_=None,
        id=None,
        active=active,
        deployed_date=deployed_date,
    )


def _mock_sdk(results, query_more_results=None):
    """Build a mock SDK whose query returns *results* on the first call
    and *query_more_results* (if any) on the second via query_more."""
    sdk = MagicMock()
    first_page = SimpleNamespace(result=results, query_token="tok" if query_more_results else None)
    sdk.deployed_package.query_deployed_package.return_value = first_page

    if query_more_results:
        second_page = SimpleNamespace(result=query_more_results, query_token=None)
        sdk.deployed_package.query_more_deployed_package.return_value = second_page

    return sdk


class TestLookupDeploymentId:
    """BUG-41: multi-result handling."""

    def test_prefers_active_over_inactive(self):
        old_active = _make_dep("dep-old-active", "true", "2025-01-01T00:00:00Z")
        newer_inactive = _make_dep("dep-newer-inactive", "false", "2026-04-01T00:00:00Z")
        sdk = _mock_sdk([newer_inactive, old_active])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result == "dep-old-active"

    def test_prefers_most_recent_among_active(self):
        old = _make_dep("dep-old", "true", "2025-01-01T00:00:00Z")
        new = _make_dep("dep-new", "true", "2026-04-01T00:00:00Z")
        sdk = _mock_sdk([old, new])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result == "dep-new"

    def test_falls_back_to_most_recent_inactive_when_no_active(self):
        old = _make_dep("dep-old", "false", "2025-01-01T00:00:00Z")
        new = _make_dep("dep-new", "false", "2026-04-01T00:00:00Z")
        sdk = _mock_sdk([old, new])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result == "dep-new"

    def test_pages_through_query_token(self):
        """The best match may be on the second page."""
        page1_dep = _make_dep("dep-page1", "false", "2025-01-01T00:00:00Z")
        page2_dep = _make_dep("dep-page2", "true", "2026-04-01T00:00:00Z")
        sdk = _mock_sdk([page1_dep], query_more_results=[page2_dep])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result == "dep-page2"

    def test_single_result_returned_as_is(self):
        dep = _make_dep("dep-only", "true", "2026-01-01T00:00:00Z")
        sdk = _mock_sdk([dep])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result == "dep-only"

    def test_no_results_returns_none(self):
        sdk = _mock_sdk([])
        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result is None

    def test_boolean_active_field(self):
        """active may come as a bool instead of a string."""
        old = _make_dep("dep-old", True, "2025-01-01T00:00:00Z")
        new = _make_dep("dep-new", False, "2026-04-01T00:00:00Z")
        sdk = _mock_sdk([new, old])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1")
        assert result == "dep-old"

    def test_active_only_returns_none_when_all_inactive(self):
        """active_only=True must return None when only inactive deployments exist,
        so that undeploy can surface its 'No active deployment found' error."""
        old = _make_dep("dep-old", "false", "2025-01-01T00:00:00Z")
        new = _make_dep("dep-new", "false", "2026-04-01T00:00:00Z")
        sdk = _mock_sdk([old, new])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1", active_only=True)
        assert result is None

    def test_active_only_selects_most_recent_active(self):
        """active_only=True still picks the newest active deployment."""
        inactive = _make_dep("dep-inactive", "false", "2026-04-01T00:00:00Z")
        old_active = _make_dep("dep-old-active", "true", "2025-01-01T00:00:00Z")
        new_active = _make_dep("dep-new-active", "true", "2026-03-01T00:00:00Z")
        sdk = _mock_sdk([inactive, old_active, new_active])

        result = _lookup_deployment_id(sdk, "pkg-1", "env-1", active_only=True)
        assert result == "dep-new-active"
