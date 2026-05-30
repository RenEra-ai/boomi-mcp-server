"""Tests for the first-/mcp-request eager KB warmup hook
(server_http.FirstRequestWarmupMiddleware).

The hook is an ASGI middleware composed INTO mcp.http_app(middleware=[...]); it
runs after the auth provider parses the principal onto the scope but BEFORE the
route-level 401, so its OWN auth-principal check is the safety boundary. These
tests drive the middleware in isolation with synthetic ASGI scopes, a fake
warmup that counts kick()s, and a stub downstream app — no MCP/auth stack or ML
deps needed. There is no pytest-asyncio plugin in this repo, so async calls are
driven via anyio.run from plain sync test functions (mirrors
test_mcp_stream_guard.py).

The end-to-end "unauthenticated /mcp POST -> 401 AND never kicked" assertion
needs the full FastMCP auth stack and is a staging/integration check (see the
plan's verification.warmup_auth_gate); here cases 6/7 prove the middleware's own
auth gate, which is the actual safety boundary.
"""
from __future__ import annotations

import sys
from pathlib import Path

import anyio

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from server_http import FirstRequestWarmupMiddleware  # noqa: E402


class _FakeWarmup:
    """Records kick() calls; no real build."""

    def __init__(self):
        self.kicks = 0

    def kick(self):
        self.kicks += 1


class _RecordingApp:
    """Downstream ASGI app that records exactly what it was called with."""

    def __init__(self):
        self.calls = []

    async def __call__(self, scope, receive, send):
        self.calls.append((scope, receive, send))


class _AuthUser:
    is_authenticated = True


class _AnonUser:
    is_authenticated = False


async def _noop_receive():
    return {"type": "http.request"}


async def _noop_send(message):
    return None


def _drive(mw, scope, receive=None, send=None):
    anyio.run(mw, scope, receive or _noop_receive, send or _noop_send)


# --- (1) lifespan scope is passed through, never kicks -----------------------

def test_lifespan_scope_does_not_kick_but_forwards():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    scope = {"type": "lifespan"}
    _drive(mw, scope)
    assert warmup.kicks == 0
    assert len(app.calls) == 1
    assert app.calls[0][0] is scope


# --- (2) non-/mcp http paths never kick --------------------------------------

def test_non_mcp_paths_do_not_kick():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    for path in ("/", "/authorize", "/token", "/web/login", "/static/app.js"):
        _drive(mw, {"type": "http", "path": path, "user": _AuthUser()})
    assert warmup.kicks == 0
    assert len(app.calls) == 5  # all forwarded


# --- (3) first authenticated /mcp request kicks exactly once -----------------

def test_first_authenticated_mcp_request_kicks_once():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    for _ in range(5):
        _drive(mw, {"type": "http", "path": "/mcp", "user": _AuthUser()})
    assert warmup.kicks == 1  # _fired flag + kick idempotency
    assert len(app.calls) == 5


def test_trailing_slash_mcp_path_matches():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    _drive(mw, {"type": "http", "path": "/mcp/", "user": _AuthUser()})
    assert warmup.kicks == 1


def test_concurrent_first_requests_kick_at_most_once():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)

    async def main():
        async with anyio.create_task_group() as tg:
            for _ in range(10):
                scope = {"type": "http", "path": "/mcp", "user": _AuthUser()}
                tg.start_soon(mw, scope, _noop_receive, _noop_send)

    anyio.run(main)
    assert warmup.kicks == 1
    assert len(app.calls) == 10


# --- (4) independent of the stream guard / session state ----------------------

def test_hook_needs_only_app_and_warmup():
    # Constructed with just (app, warmup) — no guard/session object — and works.
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    _drive(mw, {"type": "http", "path": "/mcp", "user": _AuthUser()})
    assert warmup.kicks == 1


# --- (5) scope/receive/send forwarded unchanged for all scopes ---------------

def test_scope_receive_send_forwarded_unchanged():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    scope = {"type": "http", "path": "/mcp", "user": _AuthUser()}
    _drive(mw, scope, _noop_receive, _noop_send)
    fwd_scope, fwd_recv, fwd_send = app.calls[0]
    assert fwd_scope is scope
    assert fwd_recv is _noop_receive
    assert fwd_send is _noop_send


# --- (6) unauthenticated /mcp does NOT kick (auth gate) ----------------------

def test_unauthenticated_mcp_does_not_kick():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    _drive(mw, {"type": "http", "path": "/mcp"})  # no user on scope
    _drive(mw, {"type": "http", "path": "/mcp", "user": _AnonUser()})  # unauth user
    assert warmup.kicks == 0
    assert len(app.calls) == 2  # still forwarded downstream


# --- (7) authenticated /mcp DOES kick (auth gate) ----------------------------

def test_authenticated_mcp_kicks():
    warmup, app = _FakeWarmup(), _RecordingApp()
    mw = FirstRequestWarmupMiddleware(app, warmup)
    _drive(mw, {"type": "http", "path": "/mcp", "user": _AuthUser()})
    assert warmup.kicks == 1
