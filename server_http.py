#!/usr/bin/env python3
"""
HTTP server wrapper for Boomi MCP Server with OAuth routes.

This properly exposes OAuth routes at root level alongside MCP endpoint.
"""

import os
import secrets
import uvicorn
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles

from mcp_stream_guard import (
    McpStreamGuardConfig,
    McpStreamGuardMiddleware,
    McpStreamGuardState,
    bind_fastmcp_session_manager,
    install_reaper_lifespan,
)

if __name__ == "__main__":
    # Import mcp from server module (ensures OAuth provider is initialized)
    from server import mcp

    # Get configuration from environment
    # Cloud Run provides PORT, fallback to MCP_PORT for local dev
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("MCP_PORT", "8080")))

    print(f"\n{'='*60}")
    print("🚀 Boomi MCP Server with Google OAuth 2.0")
    print(f"{'='*60}")
    print(f"Server:           http://{host}:{port}")
    print(f"🌐 Web UI:        http://{host}:{port}/")
    print(f"MCP endpoint:     /mcp")
    print(f"Web login:        /web/login (with PKCE)")
    print(f"Web callback:     /web/callback")
    print(f"OAuth authorize:  /authorize (for MCP clients)")
    print(f"OAuth callback:   /auth/callback (for MCP clients)")
    print(f"Token endpoint:   /token")
    print(f"Metadata:         /.well-known/oauth-authorization-server")
    print(f"{'='*60}")
    print("💡 To set up Boomi credentials:")
    print(f"   1. Open http://{host}:{port}/ in your browser")
    print("   2. Login with Google (uses PKCE for security)")
    print("   3. Enter your Boomi credentials in the web form")
    print(f"{'='*60}")
    print("For MCP clients: Use auth='oauth' when connecting")
    print(f"{'='*60}\n")

    # Create the HTTP app with all routes (MCP + OAuth).
    # The MCP stream cost guard is bound through http_app(middleware=[...]) so it
    # runs INSIDE the FastMCP auth middleware (bearer token already on the scope)
    # but still wraps /mcp request handling. Do NOT use app.add_middleware() for
    # it — that prepends as outermost, before auth.
    guard_config = McpStreamGuardConfig.from_env()
    guard_state = McpStreamGuardState(guard_config)
    if guard_config.enabled:
        print(
            f"[INFO] MCP stream guard enabled (get_mode={guard_config.get_mode}, "
            f"work_idle={guard_config.work_idle_seconds}s, "
            f"max_age={guard_config.max_age_seconds}s, "
            f"max_get/identity={guard_config.max_get_streams_per_identity}, "
            f"session_idle={guard_config.session_idle_seconds}s)"
        )
        app = mcp.http_app(
            middleware=[Middleware(McpStreamGuardMiddleware, state=guard_state)]
        )
        bind_fastmcp_session_manager(app, guard_state)
        install_reaper_lifespan(app, guard_state)
    else:
        print("[INFO] MCP stream guard disabled (BOOMI_MCP_STREAM_GUARD_ENABLED=false)")
        app = mcp.http_app()

    # Mount static files directory
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
        print(f"[INFO] Mounted static files from {static_dir}")

    # Add session middleware for web portal OAuth
    # MUST use persistent SESSION_SECRET for OAuth to work across requests
    session_secret = os.getenv("SESSION_SECRET")
    if not session_secret:
        print("[ERROR] SESSION_SECRET environment variable must be set!")
        print("[ERROR] Without a persistent SESSION_SECRET, OAuth will fail with 'Invalid state' errors")
        exit(1)

    print(f"[INFO] Configuring SessionMiddleware for web UI OAuth")
    app.add_middleware(
        SessionMiddleware,
        secret_key=session_secret,
        session_cookie="boomi_session",
        max_age=3600,  # 1 hour
        same_site="lax",
        https_only=os.getenv("OIDC_BASE_URL", "").startswith("https://"),
        path="/",
    )
    print(f"[INFO] SessionMiddleware configured (https_only={os.getenv('OIDC_BASE_URL', '').startswith('https://')})")

    # Run with uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")
