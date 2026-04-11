"""
Consent page CSP patch for MCP client compatibility.

Upstream FastMCP v3.1.1 uses a static CSP on the OAuth consent page.
This module patches create_consent_html to use dynamic CSP based on
the client's redirect URI:

- Claude Desktop/Code (claude.ai): Omits form-action directive because
  Electron cannot handle wildcard schemes in form-action.
- ChatGPT and other clients: Includes form-action with 'self' + wildcard
  schemes to satisfy their security validation.

Preserves behavior from vendored FastMCP fork commits 499efc8 and 9643078.
"""

import re
from urllib.parse import urlparse


def apply_consent_csp_patch():
    """Monkey-patch upstream create_consent_html with dynamic CSP logic."""
    from fastmcp.server.auth.oauth_proxy import consent

    original_create_consent_html = consent.create_consent_html

    def patched_create_consent_html(
        client_id,
        redirect_uri,
        scopes,
        txn_id,
        csrf_token,
        **kwargs,
    ):
        html = original_create_consent_html(
            client_id=client_id,
            redirect_uri=redirect_uri,
            scopes=scopes,
            txn_id=txn_id,
            csrf_token=csrf_token,
            **kwargs,
        )

        parsed = urlparse(redirect_uri)
        redirect_scheme = parsed.scheme.lower()
        redirect_netloc = parsed.netloc.lower()

        if "claude.ai" in redirect_netloc:
            # Electron can't handle wildcard schemes in form-action
            csp = (
                "default-src 'none'; "
                "style-src 'unsafe-inline'; "
                "img-src https:; "
                "base-uri 'none'"
            )
        else:
            # ChatGPT and other clients: 'self' + wildcard schemes
            form_action_parts = ["'self'", "https:", "http:"]
            if redirect_scheme and redirect_scheme not in ("http", "https"):
                form_action_parts.append(f"{redirect_scheme}:")
            form_action = " ".join(form_action_parts)
            csp = (
                "default-src 'none'; "
                "style-src 'unsafe-inline'; "
                "img-src https:; "
                "base-uri 'none'; "
                f"form-action {form_action}"
            )

        # Replace CSP in the meta tag emitted by create_page()
        html = re.sub(
            r'(<meta\s+http-equiv="Content-Security-Policy"\s+content=")[^"]*(")',
            rf"\g<1>{csp}\2",
            html,
            count=1,
        )
        return html

    consent.create_consent_html = patched_create_consent_html
