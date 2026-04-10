"""Smoke tests verifying boomi_mcp.tools is importable and CredentialStore
delegates correctly in local mode."""

import os
import sys
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)


class TestPackageImport:

    def test_import_boomi_mcp_tools_succeeds(self):
        import boomi_mcp.tools  # noqa: F401

    def test_credential_store_class_exists(self):
        from boomi_mcp.credentials import CredentialStore
        assert callable(CredentialStore)


class TestCredentialStoreLocalMode:

    @pytest.fixture(autouse=True)
    def _setup_local_env(self, tmp_path):
        self.db_file = str(tmp_path / "test_secrets.json")
        os.environ["BOOMI_LOCAL"] = "true"
        yield
        os.environ.pop("BOOMI_LOCAL", None)

    def _make_store(self):
        from boomi_mcp.credentials import CredentialStore
        return CredentialStore(db_path=self.db_file)

    def test_store_and_get_credentials(self):
        store = self._make_store()
        store.store_credentials(
            subject="user@test.com",
            profile="dev",
            username="BOOMI_TOKEN.user",
            password="secret",
            account_id="acct-1",
            base_url="https://api.boomi.com/api/rest/v1",
        )
        creds = store.get_credentials("user@test.com", "dev")
        assert creds is not None
        assert creds["username"] == "BOOMI_TOKEN.user"
        assert creds["account_id"] == "acct-1"

    def test_get_credentials_missing_returns_none(self):
        store = self._make_store()
        result = store.get_credentials("nobody@test.com", "missing")
        assert result is None

    def test_list_profiles_returns_strings(self):
        store = self._make_store()
        store.store_credentials("u@t.com", "alpha", "user", "pw", "a1")
        store.store_credentials("u@t.com", "beta", "user", "pw", "a2")
        profiles = store.list_profiles("u@t.com")
        assert isinstance(profiles, list)
        assert set(profiles) == {"alpha", "beta"}

    def test_delete_profile_returns_true(self):
        store = self._make_store()
        store.store_credentials("u@t.com", "todel", "user", "pw", "a1")
        assert store.delete_profile("u@t.com", "todel") is True

    def test_delete_missing_profile_returns_false(self):
        store = self._make_store()
        assert store.delete_profile("u@t.com", "nonexistent") is False
