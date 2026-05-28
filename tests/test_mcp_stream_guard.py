"""Tests for the MCP stream cost guard (mcp_stream_guard.py).

The guard is an ASGI middleware; there is no pytest-asyncio/anyio plugin in
this repo, so each test drives the async middleware via ``anyio.run`` from a
plain sync test function, with synthetic ASGI scope/receive/send and stub inner
apps that emit SSE frames.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import anyio
import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mcp_stream_guard import (  # noqa: E402
    McpStreamGuardConfig,
    McpStreamGuardMiddleware,
    McpStreamGuardState,
    StreamRecord,
    bind_fastmcp_session_manager,
    is_useful_sse,
)

LOGGER_NAME = "boomi.mcp_stream_guard"

# SSE frame shapes (see is_useful_sse): pings are comment lines, data is a message.
PING = b": ping - 2026-05-28T00:00:00+00:00\n\n"
DATA = b"event: message\r\ndata: {\"jsonrpc\": \"2.0\", \"method\": \"x\"}\r\n\r\n"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def cfg(**over) -> McpStreamGuardConfig:
    base = dict(
        enabled=True,
        get_mode="bounded",
        work_idle_seconds=0.2,
        max_age_seconds=10.0,
        max_get_streams_per_identity=2,
        supersede_wait_seconds=0.5,
        session_idle_seconds=0.3,
        identity_salt="testsalt",
    )
    base.update(over)
    return McpStreamGuardConfig(**base)


def make_scope(method, path="/mcp", headers=None, client_id=None, client=("203.0.113.9", 5555)):
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": list(headers or []),
        "client": client,
    }
    if client_id is not None:
        tok = type("Tok", (), {"client_id": client_id})()
        scope["user"] = type("User", (), {"access_token": tok})()
    return scope


class Recorder:
    def __init__(self):
        self.messages = []

    async def __call__(self, message):
        self.messages.append(message)

    @property
    def status(self):
        for m in self.messages:
            if m["type"] == "http.response.start":
                return m["status"]
        return None

    def header(self, name: bytes):
        for m in self.messages:
            if m["type"] == "http.response.start":
                for k, v in m["headers"]:
                    if k.lower() == name.lower():
                        return v
        return None

    @property
    def body(self):
        return b"".join(m.get("body", b"") for m in self.messages if m["type"] == "http.response.body")


async def _idle_receive():
    await anyio.sleep_forever()


def make_sse_app(frames=(), hang=True):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )
        for delay, body in frames:
            await anyio.sleep(delay)
            await send({"type": "http.response.body", "body": body, "more_body": True})
        if hang:
            await anyio.sleep_forever()

    return app


def make_repeating_sse_app(interval, body):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )
        while True:
            await anyio.sleep(interval)
            await send({"type": "http.response.body", "body": body, "more_body": True})

    return app


def make_simple_app(status=200, session_id=None, body=b'{"ok": true}'):
    async def app(scope, receive, send):
        headers = [(b"content-type", b"application/json")]
        if session_id:
            headers.append((b"mcp-session-id", session_id.encode()))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    return app


def _messages(caplog, event):
    return [r.getMessage() for r in caplog.records if r.getMessage().startswith(event)]


def _close_reason(caplog):
    msgs = _messages(caplog, "MCP_STREAM_CLOSE")
    assert msgs, "no MCP_STREAM_CLOSE logged"
    m = re.search(r"reason=(\S+)", msgs[-1])
    return m.group(1) if m else None


# --------------------------------------------------------------------------- #
# Frame classification
# --------------------------------------------------------------------------- #
def test_is_useful_sse_classification():
    assert is_useful_sse(DATA) is True
    assert is_useful_sse(b"event: message\n\n") is True
    assert is_useful_sse(PING) is False
    assert is_useful_sse(b"") is False
    assert is_useful_sse(b": keep-alive\n\n") is False


# --------------------------------------------------------------------------- #
# Identity extraction & redaction
# --------------------------------------------------------------------------- #
def test_identity_uses_authenticated_client_id():
    mw = McpStreamGuardMiddleware(None, McpStreamGuardState(cfg()))
    scope = make_scope("GET", client_id="dcr-client-abcdef")
    assert mw._identity_key(scope) == "client:dcr-client-abcdef"


def test_identity_falls_back_to_ip_and_ua_when_unauthenticated():
    mw = McpStreamGuardMiddleware(None, McpStreamGuardState(cfg()))
    scope = make_scope(
        "GET",
        headers=[(b"cf-connecting-ip", b"198.51.100.7"), (b"user-agent", b"curl/8.4")],
    )
    assert mw._identity_key(scope) == "anon:198.51.100.7|curl/8.4"


def test_logs_redact_full_client_id_and_token(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)
    long_id = "dcr-client-" + "x" * 40

    async def scenario():
        state = McpStreamGuardState(cfg(work_idle_seconds=0.15))
        mw = McpStreamGuardMiddleware(make_sse_app(), state)
        await mw(make_scope("GET", client_id=long_id), _idle_receive, Recorder())

    anyio.run(scenario)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert long_id not in text  # full client id never emitted
    assert "client_id_prefix=dcr-clie" in text  # 8-char prefix only
    assert "identity_hash=" in text


# --------------------------------------------------------------------------- #
# Work-bound GET lifetime
# --------------------------------------------------------------------------- #
def test_idle_get_closes_after_work_idle(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    async def scenario():
        state = McpStreamGuardState(cfg(work_idle_seconds=0.2, max_age_seconds=10))
        mw = McpStreamGuardMiddleware(make_sse_app(), state)
        rec = Recorder()
        await mw(make_scope("GET", client_id="cid"), _idle_receive, rec)
        return state, rec

    start = time.monotonic()
    state, rec = anyio.run(scenario)
    elapsed = time.monotonic() - start
    assert rec.status == 200
    assert 0.15 <= elapsed < 1.5
    assert _close_reason(caplog) == "work_idle"
    assert state.active_streams == {}


def test_keepalive_pings_do_not_count_as_useful_work(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    async def scenario():
        state = McpStreamGuardState(cfg(work_idle_seconds=0.2, max_age_seconds=10))
        mw = McpStreamGuardMiddleware(make_repeating_sse_app(0.05, PING), state)
        await mw(make_scope("GET", client_id="cid"), _idle_receive, Recorder())

    start = time.monotonic()
    anyio.run(scenario)
    elapsed = time.monotonic() - start
    # Pings every 0.05s but they don't reset useful-work, so it still closes at ~work_idle.
    assert _close_reason(caplog) == "work_idle"
    assert elapsed < 1.5


def test_useful_data_resets_idle_and_hard_cap_closes_stream(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    async def scenario():
        # data every 0.1s < work_idle 0.3s, so idle never fires; max_age 0.5s does.
        state = McpStreamGuardState(cfg(work_idle_seconds=0.3, max_age_seconds=0.5))
        mw = McpStreamGuardMiddleware(make_repeating_sse_app(0.1, DATA), state)
        await mw(make_scope("GET", client_id="cid"), _idle_receive, Recorder())

    anyio.run(scenario)
    assert _close_reason(caplog) == "max_age"


# --------------------------------------------------------------------------- #
# Concurrency cap & supersede
# --------------------------------------------------------------------------- #
def test_over_cap_supersedes_oldest_stream(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    async def scenario():
        state = McpStreamGuardState(
            cfg(max_get_streams_per_identity=1, work_idle_seconds=0.2, supersede_wait_seconds=1.0)
        )
        scope = make_scope("GET", client_id="cid")
        mw_old = McpStreamGuardMiddleware(make_repeating_sse_app(0.05, PING), state)
        mw_new = McpStreamGuardMiddleware(make_sse_app(), state)
        rec_new = Recorder()
        async with anyio.create_task_group() as tg:
            tg.start_soon(mw_old, scope, _idle_receive, Recorder())
            # wait for the old stream to register
            while await state.count_get_streams("client:cid") < 1:
                await anyio.sleep(0.01)
            # second GET supersedes the old one rather than being rejected
            await mw_new(scope, _idle_receive, rec_new)
            tg.cancel_scope.cancel()
        return rec_new

    rec_new = anyio.run(scenario)
    assert rec_new.status == 200  # new stream admitted (not 429)
    assert _messages(caplog, "MCP_STREAM_THRESHOLD"), "threshold not logged"
    assert _messages(caplog, "MCP_STREAM_SUPERSEDE"), "supersede not logged"
    close_reasons = [re.search(r"reason=(\S+)", m).group(1) for m in _messages(caplog, "MCP_STREAM_CLOSE")]
    assert "superseded" in close_reasons


def test_returns_429_when_victim_cannot_be_closed_in_time():
    async def scenario():
        state = McpStreamGuardState(
            cfg(max_get_streams_per_identity=1, supersede_wait_seconds=0.1)
        )
        now = anyio.current_time()
        # A stuck victim with no backing task — it never deregisters.
        victim = StreamRecord("victim", "client:cid", None, now, now, now)
        state.active_streams["victim"] = victim
        mw = McpStreamGuardMiddleware(make_sse_app(), state)
        rec = Recorder()
        await mw(make_scope("GET", client_id="cid"), _idle_receive, rec)
        return rec, victim

    rec, victim = anyio.run(scenario)
    assert rec.status == 429
    assert rec.header(b"retry-after") == b"1"
    assert victim.cancel_event.is_set()  # supersede was attempted


# --------------------------------------------------------------------------- #
# post_only kill switch
# --------------------------------------------------------------------------- #
def test_post_only_rejects_get_but_allows_post():
    async def scenario():
        state = McpStreamGuardState(cfg(get_mode="post_only"))
        get_rec = Recorder()
        await McpStreamGuardMiddleware(make_sse_app(), state)(
            make_scope("GET"), _idle_receive, get_rec
        )
        post_rec = Recorder()
        await McpStreamGuardMiddleware(make_simple_app(session_id="s1"), state)(
            make_scope("POST"), _idle_receive, post_rec
        )
        return get_rec, post_rec

    get_rec, post_rec = anyio.run(scenario)
    assert get_rec.status == 405
    assert get_rec.header(b"allow") == b"POST, DELETE"
    assert post_rec.status == 200


# --------------------------------------------------------------------------- #
# POST / DELETE session tracking
# --------------------------------------------------------------------------- #
def test_post_tracks_session_from_response_header_and_passes_through():
    async def scenario():
        state = McpStreamGuardState(cfg())
        rec = Recorder()
        await McpStreamGuardMiddleware(make_simple_app(session_id="sess-init"), state)(
            make_scope("POST"), _idle_receive, rec
        )
        return state, rec

    state, rec = anyio.run(scenario)
    assert rec.status == 200
    assert rec.body == b'{"ok": true}'
    assert "sess-init" in state.sessions


def test_post_tracks_session_from_request_header():
    async def scenario():
        state = McpStreamGuardState(cfg())
        await McpStreamGuardMiddleware(make_simple_app(), state)(
            make_scope("POST", headers=[(b"mcp-session-id", b"sess-req")]),
            _idle_receive,
            Recorder(),
        )
        return state

    state = anyio.run(scenario)
    assert "sess-req" in state.sessions


def test_delete_clears_session_and_signals_stream():
    async def scenario():
        state = McpStreamGuardState(cfg())
        now = anyio.current_time()
        state.sessions["sess-1"] = now
        rec = StreamRecord("s", "client:c", "sess-1", now, now, now)
        state.active_streams["s"] = rec
        await McpStreamGuardMiddleware(make_simple_app(status=200, body=b""), state)(
            make_scope("DELETE", headers=[(b"mcp-session-id", b"sess-1")]),
            _idle_receive,
            Recorder(),
        )
        return state, rec

    state, rec = anyio.run(scenario)
    assert "sess-1" not in state.sessions
    assert rec.cancel_event.is_set()


def test_idle_get_does_not_refresh_session_ttl():
    async def scenario():
        state = McpStreamGuardState(cfg(work_idle_seconds=0.15))
        state.sessions["s3"] = 123.0  # sentinel
        await McpStreamGuardMiddleware(make_sse_app(), state)(
            make_scope("GET", headers=[(b"mcp-session-id", b"s3")]),
            _idle_receive,
            Recorder(),
        )
        return state

    state = anyio.run(scenario)
    assert state.sessions["s3"] == 123.0  # GET never touched the session TTL


# --------------------------------------------------------------------------- #
# Session reaper
# --------------------------------------------------------------------------- #
class _FakeTransport:
    def __init__(self):
        self.terminated = False

    async def terminate(self):
        self.terminated = True


class _FakeManager:
    def __init__(self, instances):
        self._server_instances = instances


def test_reaper_terminates_idle_session_and_keeps_active(caplog):
    caplog.set_level(logging.INFO, logger=LOGGER_NAME)

    async def scenario():
        state = McpStreamGuardState(cfg(session_idle_seconds=0.2))
        transport = _FakeTransport()
        manager = _FakeManager({"s1": transport})
        state._session_manager = manager
        now = anyio.current_time()
        state.sessions["s1"] = now - 10  # stale
        state.sessions["s2"] = now  # fresh (recent POST)
        await state._reap_once()
        return state, transport, manager

    state, transport, manager = anyio.run(scenario)
    assert transport.terminated is True
    assert "s1" not in manager._server_instances
    assert "s1" not in state.sessions
    assert "s2" in state.sessions  # recent activity prevents reaping
    assert _messages(caplog, "MCP_SESSION_REAP")


# --------------------------------------------------------------------------- #
# Session manager binding
# --------------------------------------------------------------------------- #
class _FakeASGI:
    def __init__(self, manager):
        self.session_manager = manager


class _FakeAuthWrap:
    def __init__(self, app):
        self.app = app


class _FakeRoute:
    def __init__(self, path, endpoint):
        self.path = path
        self.app = endpoint
        self.endpoint = endpoint


class _FakeApp:
    def __init__(self, routes):
        self.routes = routes


def test_bind_locates_manager_in_auth_mode():
    manager = object()
    route = _FakeRoute("/mcp", _FakeAuthWrap(_FakeASGI(manager)))
    app = _FakeApp([_FakeRoute("/other", object()), route])
    state = McpStreamGuardState(cfg())
    assert bind_fastmcp_session_manager(app, state) is True
    assert state._session_manager is manager


def test_bind_locates_manager_in_local_mode():
    manager = object()
    app = _FakeApp([_FakeRoute("/mcp", _FakeASGI(manager))])
    state = McpStreamGuardState(cfg())
    assert bind_fastmcp_session_manager(app, state) is True
    assert state._session_manager is manager


def test_bind_returns_false_when_not_found():
    app = _FakeApp([_FakeRoute("/x", object())])
    state = McpStreamGuardState(cfg())
    assert bind_fastmcp_session_manager(app, state) is False
    assert state._session_manager is None


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
def test_config_from_env_defaults(monkeypatch):
    for key in (
        "BOOMI_MCP_STREAM_GUARD_ENABLED",
        "BOOMI_MCP_GET_MODE",
        "BOOMI_MCP_GET_WORK_IDLE_SECONDS",
        "BOOMI_MCP_GET_MAX_AGE_SECONDS",
        "BOOMI_MCP_MAX_GET_STREAMS_PER_IDENTITY",
        "BOOMI_MCP_SUPERSEDE_WAIT_SECONDS",
        "BOOMI_MCP_SESSION_IDLE_SECONDS",
        "BOOMI_MCP_IDENTITY_SALT",
    ):
        monkeypatch.delenv(key, raising=False)
    c = McpStreamGuardConfig.from_env()
    assert c.enabled is True
    assert c.get_mode == "bounded"
    assert c.work_idle_seconds == 45.0
    assert c.max_age_seconds == 240.0
    assert c.max_get_streams_per_identity == 2
    assert c.session_idle_seconds == 600.0
    assert c.identity_salt  # random per-process salt generated


def test_config_invalid_get_mode_falls_back(monkeypatch):
    monkeypatch.setenv("BOOMI_MCP_GET_MODE", "nonsense")
    assert McpStreamGuardConfig.from_env().get_mode == "bounded"


# --------------------------------------------------------------------------- #
# Alert setup script
# --------------------------------------------------------------------------- #
def _load_alert_module():
    path = _REPO_ROOT / "scripts" / "setup_mcp_stream_alert.py"
    spec = importlib.util.spec_from_file_location("setup_mcp_stream_alert", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_alert_metric_filter_matches_threshold_event():
    alert = _load_alert_module()
    f = alert.build_metric_filter("boomi-mcp-server")
    assert 'textPayload:"MCP_STREAM_THRESHOLD"' in f
    assert 'service_name="boomi-mcp-server"' in f


def test_alert_policy_includes_post_only_kill_switch():
    alert = _load_alert_module()
    policy = alert.build_policy_json(
        policy_name="p",
        metric_name="m",
        project="boomimcp",
        service="boomi-mcp-server",
        region="us-central1",
        threshold=5,
        duration_seconds=300,
        notification_channels=[],
    )
    text = json.dumps(policy)
    assert "BOOMI_MCP_GET_MODE=post_only" in text
    assert "logging.googleapis.com/user/m" in text


def test_alert_dry_run_does_not_call_gcloud(monkeypatch):
    alert = _load_alert_module()

    def boom(*a, **k):
        raise AssertionError("subprocess.run called during --dry-run")

    monkeypatch.setattr(alert.subprocess, "run", boom)
    assert alert.main(["--dry-run"]) == 0
