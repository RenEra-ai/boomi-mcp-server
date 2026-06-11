"""Contract tests for the invoke_boomi_api confirm_write gate (Issue #79).

Pure-unit tests against boomi_mcp.categories.meta_tools — no server import, no
real SDK calls. Covers:
- read/write classification (GET / query-POST / queryMore-POST / mutating-POST /
  PUT / DELETE);
- the fail-closed guard response shape (aligned with the deployment_utils
  remediation envelope from #10) and proof that no platform call is made;
- passthrough behavior with fakes: reads and confirmed writes execute,
  DELETE semantics (confirm_delete) are unchanged;
- the list_capabilities static catalog exposes confirm_write.
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.categories import meta_tools
from boomi_mcp.categories.meta_tools import (
    RAW_WRITE_CONFIRMATION_REQUIRED,
    _classify_raw_api_request,
    _raw_write_confirmation_guard,
    _typed_alternatives_for_endpoint,
    invoke_api,
    list_capabilities_action,
)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

class TestClassification:
    def test_get_is_read(self):
        result = _classify_raw_api_request("GET", "Component/abc")
        assert result["class"] == "read"
        assert result["requires_confirm_write"] is False

    @pytest.mark.parametrize("endpoint", [
        "Role/query",
        "/Role/query/",
        "Role/query?x=1",
        "Role/queryMore",
        "Component/QUERY",
    ])
    def test_query_posts_are_read_like(self, endpoint):
        result = _classify_raw_api_request("POST", endpoint)
        assert result["class"] == "read"
        assert result["requires_confirm_write"] is False

    def test_mutating_post_requires_confirm_write(self):
        result = _classify_raw_api_request("POST", "Branch")
        assert result["class"] == "write"
        assert result["requires_confirm_write"] is True

    def test_put_requires_confirm_write(self):
        result = _classify_raw_api_request("PUT", "Component/abc")
        assert result["class"] == "write"
        assert result["requires_confirm_write"] is True

    def test_delete_is_write_but_exempt_from_confirm_write(self):
        result = _classify_raw_api_request("DELETE", "Role/abc")
        assert result["class"] == "write"
        assert result["requires_confirm_write"] is False
        assert "confirm_delete" in result["reason"]

    def test_lowercase_method_normalized(self):
        result = _classify_raw_api_request("post", "Branch")
        assert result["requires_confirm_write"] is True

    def test_query_in_middle_of_path_is_not_read_like(self):
        # Only the FINAL segment counts — "query" elsewhere doesn't exempt.
        result = _classify_raw_api_request("POST", "query/Branch")
        assert result["requires_confirm_write"] is True


# ---------------------------------------------------------------------------
# Typed alternatives mapping
# ---------------------------------------------------------------------------

class TestTypedAlternatives:
    def test_component_family(self):
        alts = _typed_alternatives_for_endpoint("Component/abc-123")
        assert "manage_component" in alts
        assert "query_components" in alts

    def test_deployment_family(self):
        alts = _typed_alternatives_for_endpoint("DeployedPackage")
        assert alts == ["manage_deployment", "orchestrate_deploy"]

    def test_folder_family(self):
        assert _typed_alternatives_for_endpoint("Folder") == ["manage_folders"]

    def test_unknown_family_gets_discovery_tools(self):
        alts = _typed_alternatives_for_endpoint("SecretsManagerRefreshRequest")
        assert alts == ["list_capabilities", "get_schema_template", "search_boomi_docs"]

    def test_query_string_stripped(self):
        assert _typed_alternatives_for_endpoint("Folder?x=1") == ["manage_folders"]


# ---------------------------------------------------------------------------
# Guard response shape (no platform call)
# ---------------------------------------------------------------------------

class _ExplodingClient:
    """Fake Boomi client whose account access raises — proves the guard returns
    before any platform/SDK access."""

    @property
    def account(self):
        raise AssertionError("platform was touched: boomi_client.account accessed")


class TestGuardResponse:
    def test_unconfirmed_mutating_post_returns_guard(self):
        result = invoke_api(
            boomi_client=_ExplodingClient(),
            profile="test",
            endpoint="Branch",
            method="POST",
            payload='{"name": "feature-x"}',
        )
        assert result["_success"] is False
        assert result["error_code"] == RAW_WRITE_CONFIRMATION_REQUIRED
        assert result["retryable"] is False
        assert "confirm_write=true" in result["remediation"]
        assert result["method"] == "POST"
        assert result["endpoint"] == "Branch"
        assert result["classification"]["class"] == "write"
        assert result["classification"]["reason"]
        assert result["confirm_write_required"] is True
        assert result["typed_alternatives"] == ["manage_account"]
        assert result["suggested_searches"]["docs"]
        assert result["suggested_searches"]["gotchas"]

    def test_unconfirmed_put_returns_guard(self):
        result = invoke_api(
            boomi_client=_ExplodingClient(),
            profile="test",
            endpoint="Component/abc-123",
            method="PUT",
            payload="<Component/>",
            content_type="xml",
        )
        assert result["error_code"] == RAW_WRITE_CONFIRMATION_REQUIRED
        assert "manage_component" in result["typed_alternatives"]

    def test_guard_names_typed_alternatives_in_remediation(self):
        classification = _classify_raw_api_request("POST", "Folder")
        guard = _raw_write_confirmation_guard("Folder", "POST", classification)
        assert "manage_folders" in guard["remediation"]

    def test_get_does_not_hit_guard_or_require_confirmation(self):
        # GET passes classification; the exploding client proves it then reaches
        # the platform-access stage (i.e. the guard did NOT fire).
        with pytest.raises(AssertionError, match="platform was touched"):
            invoke_api(
                boomi_client=_ExplodingClient(),
                profile="test",
                endpoint="Folder/123",
                method="GET",
            )

    def test_query_post_does_not_hit_guard(self):
        with pytest.raises(AssertionError, match="platform was touched"):
            invoke_api(
                boomi_client=_ExplodingClient(),
                profile="test",
                endpoint="Role/query",
                method="POST",
                payload="{}",
            )


# ---------------------------------------------------------------------------
# Passthrough with fakes
# ---------------------------------------------------------------------------

class _FakeRequest:
    def __init__(self, record):
        self._record = record

    def set_method(self, method):
        self._record["method"] = method
        return self

    def set_body(self, body, content_type):
        self._record["body"] = body
        self._record["content_type"] = content_type
        return self


class _FakeSerializer:
    last_record = None

    def __init__(self, url, auth):
        self._record = {"url": url, "auth": auth, "headers": {}}
        _FakeSerializer.last_record = self._record

    def add_header(self, key, value):
        self._record["headers"][key] = value
        return self

    def serialize(self):
        return _FakeRequest(self._record)


class _FakeAccountService:
    def __init__(self):
        self.base_url = "https://api.example.test/api/rest/v1/acct-1"
        self.sent = []

    def get_access_token(self):
        return None

    def get_basic_auth(self):
        return None

    def send_request(self, serialized):
        self.sent.append(serialized)
        return {"ok": True}, 200, None


class _FakeClient:
    def __init__(self):
        self.account = _FakeAccountService()


@pytest.fixture
def fake_sdk(monkeypatch):
    monkeypatch.setattr(meta_tools, "Serializer", _FakeSerializer)
    client = _FakeClient()
    return client


class TestPassthrough:
    def test_get_executes_without_confirmation(self, fake_sdk):
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Folder/123", method="GET",
        )
        assert result["_success"] is True
        assert len(fake_sdk.account.sent) == 1

    def test_query_post_executes_without_confirmation(self, fake_sdk):
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Role/query", method="POST", payload="{}",
        )
        assert result["_success"] is True
        assert len(fake_sdk.account.sent) == 1

    def test_confirmed_mutating_post_executes(self, fake_sdk):
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Branch", method="POST",
            payload='{"name": "feature-x"}', confirm_write=True,
        )
        assert result["_success"] is True
        assert len(fake_sdk.account.sent) == 1
        assert _FakeSerializer.last_record["method"] == "POST"
        assert _FakeSerializer.last_record["body"] == {"name": "feature-x"}

    def test_confirmed_put_executes(self, fake_sdk):
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Component/abc", method="PUT",
            payload="<Component/>", content_type="xml", confirm_write=True,
        )
        assert result["_success"] is True
        assert len(fake_sdk.account.sent) == 1

    def test_delete_without_confirm_delete_keeps_legacy_shape(self, fake_sdk):
        # confirm_write must NOT bypass confirm_delete.
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Role/abc", method="DELETE", confirm_write=True,
        )
        assert result["_success"] is False
        assert result["error"] == "DELETE operations require explicit confirmation"
        assert result["hint"] == (
            "Re-call with confirm_delete=true after user confirms the deletion."
        )
        assert result["endpoint"] == "Role/abc"
        assert result["warning"] == "This operation may be irreversible"
        assert "error_code" not in result  # legacy DELETE shape is byte-identical
        assert len(fake_sdk.account.sent) == 0

    def test_delete_with_confirm_delete_executes_without_confirm_write(self, fake_sdk):
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Role/abc", method="DELETE", confirm_delete=True,
        )
        assert result["_success"] is True
        assert len(fake_sdk.account.sent) == 1

    def test_patch_still_rejected_before_classification(self, fake_sdk):
        result = invoke_api(
            boomi_client=fake_sdk, profile="test",
            endpoint="Role/abc", method="PATCH", confirm_write=True,
        )
        assert result["_success"] is False
        assert result["error"] == "Invalid method: PATCH"
        assert len(fake_sdk.account.sent) == 0


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def test_catalog_exposes_confirm_write():
    catalog = list_capabilities_action()
    params = catalog["tools"]["invoke_boomi_api"]["parameters"]
    assert "confirm_write" in params
    assert "confirm_write=true" in catalog["tools"]["invoke_boomi_api"]["note"]
