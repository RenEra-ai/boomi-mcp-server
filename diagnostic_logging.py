"""
Temporary diagnostic logging for OAuth token refresh debugging.

Enabled by setting BOOMI_OAUTH_DIAGNOSTICS=true in the environment.
Patches the upstream FastMCP/MCP SDK to log structured causes when:
- Client authentication fails on the /token endpoint (the silent 401)
- Client lookup returns None (storage miss or decryption failure)
- Fernet decryption fails on stored OAuth data

These patches do not change behavior -- they only add logging
at failure points that are currently silent.

Added 2026-04-11 for post-FastMCP-v3-migration cutover observability.
Remove after the new refresh path is proven stable (~72h post-deploy).
"""

import logging

logger = logging.getLogger("boomi.oauth_diagnostic")


def apply_token_handler_logging():
    """Patch TokenHandler.handle to log client auth failures.

    The MCP SDK's TokenHandler catches AuthenticationError and returns
    401 without logging the reason. This patch intercepts
    authenticate_request to log the failure cause, then lets the
    original handle() run exactly once (no double auth).
    """
    from mcp.server.auth.middleware.client_auth import (
        AuthenticationError,
        ClientAuthenticator,
    )

    original_authenticate = ClientAuthenticator.authenticate_request

    async def patched_authenticate(self, request):
        try:
            return await original_authenticate(self, request)
        except AuthenticationError as e:
            form_data = await request.form()
            client_id = form_data.get("client_id", "<missing>")
            grant_type = form_data.get("grant_type", "<missing>")
            logger.warning(
                "Token endpoint client auth FAILED: %s "
                "(client_id=%s, grant_type=%s)",
                e.message,
                client_id[:16] + "..." if client_id and len(client_id) > 16 else client_id,
                grant_type,
            )
            raise  # re-raise unchanged so TokenHandler returns 401 normally

    ClientAuthenticator.authenticate_request = patched_authenticate
    logger.info("Patched ClientAuthenticator.authenticate_request with diagnostic logging")


def apply_client_lookup_logging(auth_provider):
    """Patch the OAuthProxy.get_client to log lookup failures.

    When get_client returns None, the ClientAuthenticator raises
    AuthenticationError("Invalid client_id") but never logs WHY
    the lookup failed (not found? decryption error? deserialization?).
    """
    original_get_client = auth_provider.get_client.__func__

    async def patched_get_client(self, client_id):
        try:
            result = await original_get_client(self, client_id)
            if result is None:
                logger.warning(
                    "get_client returned None for client_id=%s "
                    "(not found in storage or decryption failed)",
                    client_id[:16] + "..." if client_id and len(client_id) > 16 else client_id,
                )
            else:
                logger.debug(
                    "get_client found client_id=%s",
                    client_id[:16] + "..." if client_id and len(client_id) > 16 else client_id,
                )
            return result
        except Exception as e:
            logger.error(
                "get_client raised %s for client_id=%s: %s",
                type(e).__name__,
                client_id[:16] + "..." if client_id and len(client_id) > 16 else client_id,
                e,
            )
            raise

    import types
    auth_provider.get_client = types.MethodType(patched_get_client, auth_provider)
    logger.info("Patched auth_provider.get_client with diagnostic logging")


def apply_storage_get_logging(encrypted_storage):
    """Patch VerifiedStorage/FernetEncryptionWrapper get() to log failures.

    Distinguishes between:
    - Key not found in MongoDB (returns None)
    - Fernet decryption failed (raises InvalidToken)
    - Deserialization failed (raises ValidationError)
    """
    original_get = encrypted_storage.get

    async def patched_get(*, key, **kwargs):
        collection = kwargs.get("collection", "<default>")
        key_prefix = key[:12] + "..." if key and len(key) > 12 else key
        try:
            result = await original_get(key=key, **kwargs)
            if result is None:
                logger.debug(
                    "Storage GET miss: key=%s collection=%s",
                    key_prefix,
                    collection,
                )
            return result
        except Exception as e:
            logger.error(
                "Storage GET error: key=%s collection=%s error=%s: %s",
                key_prefix,
                collection,
                type(e).__name__,
                e,
            )
            raise

    encrypted_storage.get = patched_get
    logger.info("Patched encrypted_storage.get with diagnostic logging")


def apply_all_patches(auth_provider, encrypted_storage):
    """Apply all diagnostic logging patches."""
    logger.setLevel(logging.DEBUG)

    apply_token_handler_logging()
    apply_client_lookup_logging(auth_provider)
    apply_storage_get_logging(encrypted_storage)

    logger.info("All OAuth diagnostic patches applied")
