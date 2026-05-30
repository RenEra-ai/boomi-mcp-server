"""Tests for server_http.build_mcp_app — the FastMCP HTTP app constructor that
selects stateful vs stateless streamable-HTTP transport (Workstream B).

build_mcp_app is the unit-testable extraction of the app-construction block that
used to live inside ``if __name__ == "__main__":``. These tests drive it with a
fake ``mcp`` that records the kwargs passed to ``http_app`` and with the
guard-binding helpers monkeypatched to recorders, so no uvicorn / MCP / auth
stack is needed.

Transport contract:
  * stateless_http=False (default) preserves the original behavior exactly:
    stream guard middleware (when enabled) + the three bindings
    (bind_fastmcp_session_manager, bind_auth_jwt_issuer, install_reaper_lifespan),
    and http_app is called with stateless_http=False, json_response=False.
  * stateless_http=True passes stateless_http=True and json_response=<flag> to
    http_app, installs NO stream guard, and performs NONE of the three bindings.
  * The eager warmup hook (FirstRequestWarmupMiddleware) is installed as the
    outermost custom middleware in BOTH modes iff kb_warmup is present AND
    kb_warmup_eager is set.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import server_http  # noqa: E402
from server_http import (  # noqa: E402
    FirstRequestWarmupMiddleware,
    build_mcp_app,
)
from mcp_stream_guard import McpStreamGuardMiddleware  # noqa: E402


class _FakeApp:
    """Stand-in for the Starlette app returned by mcp.http_app."""


class _FakeMcp:
    """Records the kwargs passed to http_app; returns a throwaway app."""

    def __init__(self):
        self.http_app_calls = []

    def http_app(self, **kwargs):
        self.http_app_calls.append(kwargs)
        return _FakeApp()


class _FakeWarmup:
    """Truthy warmup object; build_mcp_app only checks identity/None, never builds."""


@pytest.fixture()
def recorded_bindings(monkeypatch):
    """Replace the three guard-binding helpers with recorders so the stateful
    path can be asserted without a real FastMCP app."""
    calls = {"session_manager": [], "jwt_issuer": [], "reaper": []}
    monkeypatch.setattr(
        server_http, "bind_fastmcp_session_manager",
        lambda app, state: calls["session_manager"].append((app, state)),
    )
    monkeypatch.setattr(
        server_http, "bind_auth_jwt_issuer",
        lambda app, state: calls["jwt_issuer"].append((app, state)),
    )
    monkeypatch.setattr(
        server_http, "install_reaper_lifespan",
        lambda app, state: calls["reaper"].append((app, state)),
    )
    return calls


def _middleware_classes(http_app_call):
    return [m.cls for m in (http_app_call.get("middleware") or [])]


# --- transport flag wiring -------------------------------------------------

def test_defaults_pass_stateful_false_false(monkeypatch, recorded_bindings):
    monkeypatch.setenv("BOOMI_MCP_STREAM_GUARD_ENABLED", "true")
    mcp = _FakeMcp()
    build_mcp_app(mcp)
    assert len(mcp.http_app_calls) == 1
    call = mcp.http_app_calls[0]
    assert call["stateless_http"] is False
    assert call["json_response"] is False


def test_stateless_json_false(monkeypatch, recorded_bindings):
    mcp = _FakeMcp()
    build_mcp_app(mcp, stateless_http=True, json_response=False)
    call = mcp.http_app_calls[0]
    assert call["stateless_http"] is True
    assert call["json_response"] is False


def test_stateless_json_true(monkeypatch, recorded_bindings):
    mcp = _FakeMcp()
    build_mcp_app(mcp, stateless_http=True, json_response=True)
    call = mcp.http_app_calls[0]
    assert call["stateless_http"] is True
    assert call["json_response"] is True


# --- stateless mode skips guard + all bindings -----------------------------

def test_stateless_skips_guard_and_bindings(monkeypatch, recorded_bindings):
    mcp = _FakeMcp()
    build_mcp_app(mcp, stateless_http=True)
    call = mcp.http_app_calls[0]
    assert McpStreamGuardMiddleware not in _middleware_classes(call)
    assert recorded_bindings["session_manager"] == []
    assert recorded_bindings["jwt_issuer"] == []
    assert recorded_bindings["reaper"] == []


# --- stateful mode preserves guard + bindings exactly ----------------------

def test_stateful_installs_guard_and_bindings(monkeypatch, recorded_bindings):
    monkeypatch.setenv("BOOMI_MCP_STREAM_GUARD_ENABLED", "true")
    mcp = _FakeMcp()
    app = build_mcp_app(mcp)
    call = mcp.http_app_calls[0]
    assert McpStreamGuardMiddleware in _middleware_classes(call)
    # each binding called exactly once, against the returned app
    assert len(recorded_bindings["session_manager"]) == 1
    assert len(recorded_bindings["jwt_issuer"]) == 1
    assert len(recorded_bindings["reaper"]) == 1
    assert recorded_bindings["session_manager"][0][0] is app
    assert recorded_bindings["jwt_issuer"][0][0] is app
    assert recorded_bindings["reaper"][0][0] is app


def test_stateful_guard_disabled_omits_guard_and_bindings(monkeypatch, recorded_bindings):
    monkeypatch.setenv("BOOMI_MCP_STREAM_GUARD_ENABLED", "false")
    mcp = _FakeMcp()
    build_mcp_app(mcp)
    call = mcp.http_app_calls[0]
    # guard disabled: no guard middleware, no bindings, but still stateful framing
    assert McpStreamGuardMiddleware not in _middleware_classes(call)
    assert call["stateless_http"] is False
    assert call["json_response"] is False
    assert recorded_bindings["session_manager"] == []
    assert recorded_bindings["jwt_issuer"] == []
    assert recorded_bindings["reaper"] == []


# --- warmup middleware gating ----------------------------------------------

def test_warmup_present_in_stateless_when_enabled(monkeypatch, recorded_bindings):
    mcp = _FakeMcp()
    build_mcp_app(
        mcp, stateless_http=True, kb_warmup=_FakeWarmup(), kb_warmup_eager=True
    )
    classes = _middleware_classes(mcp.http_app_calls[0])
    assert classes[0] is FirstRequestWarmupMiddleware  # outermost custom middleware


def test_warmup_outermost_before_guard_in_stateful(monkeypatch, recorded_bindings):
    monkeypatch.setenv("BOOMI_MCP_STREAM_GUARD_ENABLED", "true")
    mcp = _FakeMcp()
    build_mcp_app(mcp, kb_warmup=_FakeWarmup(), kb_warmup_eager=True)
    classes = _middleware_classes(mcp.http_app_calls[0])
    assert classes == [FirstRequestWarmupMiddleware, McpStreamGuardMiddleware]


def test_warmup_omitted_when_no_warmup(monkeypatch, recorded_bindings):
    mcp = _FakeMcp()
    build_mcp_app(mcp, stateless_http=True, kb_warmup=None, kb_warmup_eager=True)
    classes = _middleware_classes(mcp.http_app_calls[0])
    assert FirstRequestWarmupMiddleware not in classes


def test_warmup_omitted_when_eager_off(monkeypatch, recorded_bindings):
    mcp = _FakeMcp()
    build_mcp_app(
        mcp, stateless_http=True, kb_warmup=_FakeWarmup(), kb_warmup_eager=False
    )
    classes = _middleware_classes(mcp.http_app_calls[0])
    assert FirstRequestWarmupMiddleware not in classes


# --- _flag true-value convention -------------------------------------------

@pytest.mark.parametrize("value", ["true", "1", "yes", "on", "TRUE", "  On  "])
def test_flag_true_values(monkeypatch, value):
    monkeypatch.setenv("BOOMI_MCP_STATELESS_HTTP", value)
    assert server_http._flag("BOOMI_MCP_STATELESS_HTTP", "false") is True


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "maybe"])
def test_flag_false_values(monkeypatch, value):
    monkeypatch.setenv("BOOMI_MCP_STATELESS_HTTP", value)
    assert server_http._flag("BOOMI_MCP_STATELESS_HTTP", "true") is False


def test_flag_default_used_when_unset(monkeypatch):
    monkeypatch.delenv("BOOMI_MCP_STATELESS_HTTP", raising=False)
    assert server_http._flag("BOOMI_MCP_STATELESS_HTTP", "false") is False
    assert server_http._flag("BOOMI_MCP_STATELESS_HTTP", "true") is True
