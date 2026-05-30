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
    DEFAULT_MCP_PATH,
    McpStreamGuardConfig,
    McpStreamGuardMiddleware,
    McpStreamGuardState,
    bind_auth_jwt_issuer,
    bind_fastmcp_session_manager,
    install_reaper_lifespan,
)


class FirstRequestWarmupMiddleware:
    """One-shot ASGI hook: kick the deferred KB warmup on the first AUTHENTICATED
    /mcp request.

    Composed INTO ``mcp.http_app(middleware=[...])`` (NOT app.add_middleware /
    an outer wrapper), so it runs as APP-LEVEL middleware: after the auth
    provider has parsed the bearer principal onto the scope, but BEFORE the
    route-level RequireAuthMiddleware that enforces the 401. It is therefore NOT
    behind the 401 — an unauthenticated /mcp request still reaches it — so it
    must self-check the authenticated principal and kick ONLY when authenticated.
    That keeps the expensive ML warmup from running for unauthenticated callers.

    Gating:
      * scope['type'] == 'http' — pass through 'lifespan' (sent pre-bind;
        kicking there would recreate the import-time contention this avoids) and
        any other scope.
      * path is the /mcp endpoint — never web UI / OAuth / static traffic.
      * the parsed principal is authenticated (mirrors the route-level 401's
        ``isinstance(scope['user'], AuthenticatedUser)`` via the Starlette
        ``user.is_authenticated`` contract).

    Eager warmup is opportunistic only; correctness rests on KbWarmup.get() at
    the first tool call. The single-build guarantee is KbWarmup.kick()'s
    lock-guarded idempotency, so a benign race between two first requests (or the
    unsynchronized _fired flag) cannot start two builds.
    """

    def __init__(self, app, warmup, mcp_path=DEFAULT_MCP_PATH):
        self.app = app
        self._warmup = warmup
        self._mcp_path = (mcp_path or DEFAULT_MCP_PATH).rstrip("/") or DEFAULT_MCP_PATH
        self._fired = False

    def _is_mcp_path(self, scope):
        return (scope.get("path") or "").rstrip("/") == self._mcp_path

    @staticmethod
    def _is_authenticated(scope):
        # Starlette's AuthenticationMiddleware (added by the FastMCP auth
        # provider) sets scope['user']; AuthenticatedUser.is_authenticated is
        # True, UnauthenticatedUser.is_authenticated is False. Absent user
        # (no auth configured) -> treat as unauthenticated and do not kick.
        return bool(getattr(scope.get("user"), "is_authenticated", False))

    async def __call__(self, scope, receive, send):
        if (
            not self._fired
            and scope.get("type") == "http"
            and self._is_mcp_path(scope)
            and self._is_authenticated(scope)
        ):
            self._fired = True
            try:
                self._warmup.kick()
            except Exception as e:  # noqa: BLE001 — never break request handling
                print(f"[WARNING] eager KB warmup kick failed: {e}")
        await self.app(scope, receive, send)


def _flag(name, default):
    """Read a boolean env flag using the same true-value convention as
    ``mcp_stream_guard._flag`` / ``server._kb_env_flag`` (case-insensitive,
    stripped): true, 1, yes, on."""
    return os.getenv(name, default).strip().lower() in ("true", "1", "yes", "on")


def build_mcp_app(
    mcp,
    *,
    kb_warmup=None,
    kb_warmup_eager=False,
    stateless_http=False,
    json_response=False,
):
    """Construct the FastMCP HTTP app (the /mcp routes + custom middleware).

    Two transport modes, selected by ``stateless_http``:

    * Stateful (default, ``stateless_http=False``) — preserves the original
      behavior exactly: the eager KB warmup hook (when enabled) plus the MCP
      stream cost guard, with ``bind_fastmcp_session_manager``,
      ``bind_auth_jwt_issuer``, and ``install_reaper_lifespan`` wired onto the
      app. ``stateless_http=False`` / ``json_response=False`` are passed to
      ``http_app`` explicitly — these are the FastMCP defaults, so this is
      behaviorally identical to not passing them.

    * Stateless (``stateless_http=True``) — runs FastMCP streamable HTTP without
      per-instance session state (eliminates the post-redeploy ``404 Session not
      found``). The stream guard, session-manager binding, JWT-issuer binding,
      and session reaper are all skipped — they only make sense for stateful
      sessions. ``json_response`` is honored only in this mode (it changes POST
      response framing and must be validated independently).

    The eager warmup hook is installed (outermost custom middleware) in BOTH
    modes when ``kb_warmup`` is present and ``kb_warmup_eager`` is set.
    """
    # Eager warmup hook: installed only when the KB is enabled (warmup present)
    # AND eager warmup is on. It is opportunistic — get() at the first tool call
    # remains the correctness path. Placed first (outermost custom middleware)
    # so it observes every authenticated /mcp request before the guard, then
    # forwards untouched.
    warmup_mw = []
    if kb_warmup is not None and kb_warmup_eager:
        warmup_mw = [Middleware(FirstRequestWarmupMiddleware, warmup=kb_warmup)]
        print("[INFO] Eager KB warmup hook enabled (first authenticated /mcp request)")

    if stateless_http:
        print(
            f"[INFO] MCP stateless HTTP transport ENABLED "
            f"(stateless_http=True, json_response={json_response}); stream guard, "
            f"session-manager binding, JWT-issuer binding, and session reaper are "
            f"disabled in stateless mode"
        )
        return mcp.http_app(
            stateless_http=True,
            json_response=json_response,
            middleware=warmup_mw,
        )

    print("[INFO] MCP stateful HTTP transport (stateless_http=False)")
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
            stateless_http=False,
            json_response=False,
            middleware=warmup_mw
            + [Middleware(McpStreamGuardMiddleware, state=guard_state)],
        )
        bind_fastmcp_session_manager(app, guard_state)
        bind_auth_jwt_issuer(app, guard_state)
        install_reaper_lifespan(app, guard_state)
    else:
        print("[INFO] MCP stream guard disabled (BOOMI_MCP_STREAM_GUARD_ENABLED=false)")
        app = mcp.http_app(
            stateless_http=False,
            json_response=False,
            middleware=warmup_mw,
        )
    return app


if __name__ == "__main__":
    # Import mcp from server module (ensures OAuth provider is initialized).
    # `import server` (not just the from-import) lets us reach the optional
    # _kb_warmup / _kb_warmup_eager attributes, which exist only when
    # BOOMI_DOCS_ENABLED is set.
    import server
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
    # The MCP stream cost guard and the eager KB warmup hook are bound through
    # http_app(middleware=[...]) so they run INSIDE the FastMCP auth middleware
    # (bearer token already parsed onto the scope) but still wrap /mcp request
    # handling. Do NOT use app.add_middleware() for them — that prepends as
    # outermost, before auth, so the warmup hook could not see the principal and
    # could fire for unauthenticated callers.
    #
    # Transport mode is selected by BOOMI_MCP_STATELESS_HTTP (default off).
    # BOOMI_MCP_JSON_RESPONSE (default off) is honored only in stateless mode.
    # Both default false, so production behavior is unchanged until the flags are
    # explicitly enabled (see build_mcp_app for the full contract).
    app = build_mcp_app(
        mcp,
        kb_warmup=getattr(server, "_kb_warmup", None),
        kb_warmup_eager=getattr(server, "_kb_warmup_eager", False),
        stateless_http=_flag("BOOMI_MCP_STATELESS_HTTP", "false"),
        json_response=_flag("BOOMI_MCP_JSON_RESPONSE", "false"),
    )

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
