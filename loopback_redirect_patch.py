"""
RFC 8252 §7.3 loopback redirect URI patch.

Upstream FastMCP v3.1.1's matches_allowed_pattern performs strict port
matching, so a CIMD redirect_uri of "http://localhost/callback" only
matches port 80. RFC 8252 §7.3 requires authorization servers to accept
ANY port for loopback redirect URIs ("localhost", "127.0.0.1", "::1"),
which is exactly what Claude Code's CIMD doc relies on:

    {
      "redirect_uris": [
        "http://localhost/callback",
        "http://127.0.0.1/callback"
      ]
    }

Without this patch, Claude Code authentication fails with:
    "Redirect URI 'http://localhost:53002/callback' does not match
     CIMD redirect_uris."

The patch relaxes port matching for loopback hosts only. All other
host checks (userinfo rejection, scheme, host, path) remain upstream.
"""

from urllib.parse import urlparse

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def apply_loopback_redirect_patch() -> None:
    """Monkey-patch FastMCP to honor RFC 8252 loopback port flexibility."""
    from fastmcp.server.auth import redirect_validation
    from fastmcp.server.auth.oauth_proxy import models as proxy_models

    original = redirect_validation.matches_allowed_pattern

    def patched(uri: str, pattern: str) -> bool:
        if original(uri, pattern):
            return True

        try:
            uri_parsed = urlparse(uri)
            pattern_parsed = urlparse(pattern)
        except ValueError:
            return False

        # Preserve upstream userinfo bypass-attack protection.
        if uri_parsed.username is not None or uri_parsed.password is not None:
            return False

        if uri_parsed.scheme.lower() != pattern_parsed.scheme.lower():
            return False

        uri_host = (uri_parsed.hostname or "").lower()
        pattern_host = (pattern_parsed.hostname or "").lower()

        # Only relax matching for loopback hosts.
        if uri_host != pattern_host or uri_host not in LOOPBACK_HOSTS:
            return False

        # Pattern must omit an explicit port — the "any loopback port" signal.
        if pattern_parsed.port is not None:
            return False

        if (uri_parsed.path or "/") != (pattern_parsed.path or "/"):
            return False

        return True

    redirect_validation.matches_allowed_pattern = patched
    # Re-bind the symbol already imported into the oauth_proxy.models namespace.
    proxy_models.matches_allowed_pattern = patched
