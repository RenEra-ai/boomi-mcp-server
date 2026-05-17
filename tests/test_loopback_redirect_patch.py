"""Tests for the RFC 8252 §7.3 loopback redirect URI patch."""

import importlib
import sys

import pytest


@pytest.fixture
def patched():
    """Re-import the affected modules, apply the patch, and yield refs."""
    # Make sure we patch a fresh copy in case other tests touched it.
    from fastmcp.server.auth import redirect_validation
    from fastmcp.server.auth.oauth_proxy import models as proxy_models

    redirect_validation = importlib.reload(redirect_validation)
    proxy_models = importlib.reload(proxy_models)

    # Drop any cached import of the patch so apply runs against fresh modules.
    sys.modules.pop("loopback_redirect_patch", None)
    from loopback_redirect_patch import apply_loopback_redirect_patch

    apply_loopback_redirect_patch()
    yield redirect_validation.matches_allowed_pattern, proxy_models.matches_allowed_pattern


@pytest.mark.parametrize(
    "uri,pattern,expected",
    [
        # Claude Code's CIMD redirect_uris (port-less localhost / 127.0.0.1)
        ("http://localhost:53002/callback", "http://localhost/callback", True),
        ("http://localhost:3118/callback", "http://localhost/callback", True),
        ("http://127.0.0.1:53002/callback", "http://127.0.0.1/callback", True),
        # IPv6 loopback
        ("http://[::1]:9999/callback", "http://[::1]/callback", True),
        # Explicit wildcards still work (upstream behavior preserved)
        ("http://localhost:53002/callback", "http://localhost:*/callback", True),
        # Exact match still works
        ("http://localhost/callback", "http://localhost/callback", True),
        # Path mismatch must still fail
        ("http://localhost:53002/evil", "http://localhost/callback", False),
        # Non-loopback hosts: strict port matching still applies (no relaxation)
        ("http://example.com:8080/callback", "http://example.com/callback", False),
        # Userinfo bypass attack must still be blocked
        ("http://localhost@evil.com:53002/callback", "http://localhost/callback", False),
        # Scheme mismatch must still fail
        ("https://localhost:53002/callback", "http://localhost/callback", False),
    ],
)
def test_loopback_port_flexibility(patched, uri, pattern, expected):
    matches_in_module, matches_in_proxy = patched
    assert matches_in_module(uri, pattern) is expected
    # The oauth_proxy.models namespace must also see the patched symbol — that's
    # the call site CIMD redirect validation actually goes through.
    assert matches_in_proxy(uri, pattern) is expected


def test_oauth_proxy_cimd_validation_accepts_loopback_port(patched):
    """End-to-end: ProxyDCRClient should accept a Claude-Code-style request."""
    from mcp.shared.auth import OAuthClientInformationFull
    from pydantic import AnyUrl

    from fastmcp.server.auth.cimd import CIMDDocument
    from fastmcp.server.auth.oauth_proxy.models import ProxyDCRClient

    cimd_doc = CIMDDocument(
        client_id="https://claude.ai/oauth/claude-code-client-metadata",
        client_name="Claude Code",
        redirect_uris=[
            "http://localhost/callback",
            "http://127.0.0.1/callback",
        ],
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        token_endpoint_auth_method="none",
    )

    client = ProxyDCRClient(
        client_id="https://claude.ai/oauth/claude-code-client-metadata",
        redirect_uris=[AnyUrl("http://localhost/callback")],
        cimd_document=cimd_doc,
        allowed_redirect_uri_patterns=None,
    )

    resolved = client.validate_redirect_uri(AnyUrl("http://localhost:53002/callback"))
    assert str(resolved).startswith("http://localhost:53002/callback")
