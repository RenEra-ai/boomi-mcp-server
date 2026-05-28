#!/usr/bin/env python3
"""MCP stream cost guard.

Server-side guardrails so buggy or orphaned MCP clients cannot keep unbounded
standalone ``GET /mcp`` SSE streams open on request-billed Cloud Run. See
``docs/plans/mcp_stream_cost_guard_plan_2026-05-28.json`` and the implementation
plan for the full rationale.

Stance: keep ``GET /mcp`` enabled by default but make it *identity-budgeted* and
*work-bound*. ``BOOMI_MCP_GET_MODE=post_only`` is an emergency kill switch.

The guard is a Starlette/ASGI middleware bound through
``mcp.http_app(middleware=[...])`` so it runs *inside* the FastMCP auth
middleware (the bearer token is already decoded onto ``scope["user"]``) but
still wraps the ``/mcp`` request handling.
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import logging
import os
import secrets
from dataclasses import dataclass, field
from typing import Optional

import anyio

try:  # mirror FastMCP's pre-409 protocol-version validation precisely
    from mcp.shared.version import SUPPORTED_PROTOCOL_VERSIONS as _SUPPORTED_PROTOCOL_VERSIONS
    from mcp.types import DEFAULT_NEGOTIATED_VERSION as _DEFAULT_NEGOTIATED_VERSION
except Exception:  # pragma: no cover - defensive; treat any version as acceptable
    _SUPPORTED_PROTOCOL_VERSIONS = None
    _DEFAULT_NEGOTIATED_VERSION = None

logger = logging.getLogger("boomi.mcp_stream_guard")

# Default streamable-http path mounted by FastMCP.
DEFAULT_MCP_PATH = "/mcp"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class McpStreamGuardConfig:
    """Runtime configuration, populated from environment variables."""

    enabled: bool = True
    get_mode: str = "bounded"  # "bounded" | "post_only"
    work_idle_seconds: float = 45.0
    max_age_seconds: float = 240.0
    max_get_streams_per_identity: int = 2
    supersede_wait_seconds: float = 2.0
    session_idle_seconds: float = 600.0
    identity_salt: str = ""

    @classmethod
    def from_env(cls) -> "McpStreamGuardConfig":
        def _flag(name: str, default: str) -> bool:
            return os.getenv(name, default).strip().lower() in ("true", "1", "yes", "on")

        def _float(name: str, default: str) -> float:
            try:
                return float(os.getenv(name, default))
            except (TypeError, ValueError):
                return float(default)

        def _int(name: str, default: str) -> int:
            try:
                return int(os.getenv(name, default))
            except (TypeError, ValueError):
                return int(default)

        mode = os.getenv("BOOMI_MCP_GET_MODE", "bounded").strip().lower()
        if mode not in ("bounded", "post_only"):
            logger.warning(
                "MCP_STREAM_GUARD invalid BOOMI_MCP_GET_MODE=%r; defaulting to 'bounded'",
                mode,
            )
            mode = "bounded"

        salt = os.getenv("BOOMI_MCP_IDENTITY_SALT", "").strip() or secrets.token_hex(16)

        return cls(
            enabled=_flag("BOOMI_MCP_STREAM_GUARD_ENABLED", "true"),
            get_mode=mode,
            work_idle_seconds=_float("BOOMI_MCP_GET_WORK_IDLE_SECONDS", "45"),
            max_age_seconds=_float("BOOMI_MCP_GET_MAX_AGE_SECONDS", "240"),
            max_get_streams_per_identity=_int("BOOMI_MCP_MAX_GET_STREAMS_PER_IDENTITY", "2"),
            supersede_wait_seconds=_float("BOOMI_MCP_SUPERSEDE_WAIT_SECONDS", "2"),
            session_idle_seconds=_float("BOOMI_MCP_SESSION_IDLE_SECONDS", "600"),
            identity_salt=salt,
        )


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #
@dataclass
class StreamRecord:
    """One tracked standalone GET /mcp SSE stream."""

    stream_id: str
    identity_key: str
    session_id: Optional[str]
    started_at: float
    last_useful: float
    last_post: float
    cancel_event: anyio.Event = field(default_factory=anyio.Event)


class McpStreamGuardState:
    """Per-instance, in-memory registries for active streams and sessions.

    Budgeting is intentionally per-Cloud-Run-instance for the MVP (the incident
    stayed on one instance). Distributed budgeting can be layered on later.
    """

    def __init__(
        self,
        config: McpStreamGuardConfig,
        *,
        mcp_path: str = DEFAULT_MCP_PATH,
    ) -> None:
        self.config = config
        self.mcp_path = mcp_path.rstrip("/") or DEFAULT_MCP_PATH
        self.active_streams: dict[str, StreamRecord] = {}
        self.sessions: dict[str, float] = {}
        # Transient count of POSTs currently being processed, per session id.
        # An in-flight POST is treated as liveness (see has_inflight) without
        # granting persistent activity credit, so it clears as soon as the POST
        # returns regardless of the response status.
        self.inflight: dict[str, int] = {}
        self._session_manager = None  # bound post-construction
        # FastMCP OAuth-proxy JWT issuer, bound post-construction. Used to decode
        # the raw bearer JWT and budget by the per-DCR-client client_id.
        self.jwt_issuer = None
        self._lock = anyio.Lock()
        self._salt = config.identity_salt.encode()

    # -- identity helpers ---------------------------------------------------- #
    def identity_hash(self, identity_key: str) -> str:
        return hmac.new(self._salt, identity_key.encode(), hashlib.sha256).hexdigest()[:12]

    # -- registry helpers (callers hold no lock; methods lock internally) ---- #
    async def count_get_streams(self, identity_key: str) -> int:
        async with self._lock:
            return sum(1 for r in self.active_streams.values() if r.identity_key == identity_key)

    async def touch_post(self, session_id: Optional[str], now: float) -> None:
        if not session_id:
            return
        async with self._lock:
            self.sessions[session_id] = now
            for record in self.active_streams.values():
                if record.session_id == session_id:
                    record.last_post = now

    async def inflight_inc(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        async with self._lock:
            self.inflight[session_id] = self.inflight.get(session_id, 0) + 1

    async def inflight_dec(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        async with self._lock:
            remaining = self.inflight.get(session_id, 0) - 1
            if remaining > 0:
                self.inflight[session_id] = remaining
            else:
                self.inflight.pop(session_id, None)

    def has_inflight(self, session_id: Optional[str]) -> bool:
        """True if a POST is currently processing for this session. Read without
        the lock (single-threaded event loop; dict.get is atomic and there is no
        await), so the GET watchdog can consult it cheaply on every poll."""
        if not session_id:
            return False
        return self.inflight.get(session_id, 0) > 0

    async def forget_session(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        async with self._lock:
            self.sessions.pop(session_id, None)
            self.inflight.pop(session_id, None)
            for record in self.active_streams.values():
                if record.session_id == session_id:
                    record.cancel_event.set()

    # -- session reaping ----------------------------------------------------- #
    async def run_session_reaper(self) -> None:
        cfg = self.config
        interval = max(1.0, min(30.0, cfg.session_idle_seconds / 4 if cfg.session_idle_seconds else 30.0))
        while True:
            await anyio.sleep(interval)
            try:
                await self._reap_once()
            except Exception:  # pragma: no cover - defensive
                logger.exception("MCP_STREAM_GUARD session reaper error")

    async def _reap_once(self) -> None:
        cutoff = self.config.session_idle_seconds
        async with self._lock:
            now = anyio.current_time()
            stale = [
                sid for sid, last in self.sessions.items()
                if now - last >= cutoff and self.inflight.get(sid, 0) <= 0
            ]
        for session_id in stale:
            await self._terminate_session(session_id)

    async def _terminate_session(self, session_id: str) -> None:
        cutoff = self.config.session_idle_seconds
        # Re-check under the lock right before terminating: a POST may have
        # refreshed this session, or one may now be in flight, after the stale
        # snapshot was taken. Remove it from tracking atomically so we only
        # terminate a still-idle session with no in-flight work.
        async with self._lock:
            last = self.sessions.get(session_id)
            if last is None:
                return  # already gone
            if anyio.current_time() - last < cutoff or self.inflight.get(session_id, 0) > 0:
                return  # became active again after the snapshot; leave it alone
            self.sessions.pop(session_id, None)

        transport = None
        manager = self._session_manager
        instances = getattr(manager, "_server_instances", None) if manager is not None else None
        if isinstance(instances, dict):
            transport = instances.get(session_id)
        if transport is not None:
            try:
                await transport.terminate()
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "MCP_STREAM_GUARD failed to terminate session_id_prefix=%s", session_id[:8]
                )
            if isinstance(instances, dict):
                instances.pop(session_id, None)
        # The SDK transport for this id is now gone. If a racing accepted POST
        # re-added local tracking (or in-flight count) for the same id while
        # terminate() was pending, that tracking is stale — the SDK would 404 it
        # — so drop it to avoid treating a dead session as established.
        async with self._lock:
            self.sessions.pop(session_id, None)
            self.inflight.pop(session_id, None)
        logger.info(
            "MCP_SESSION_REAP session_id_prefix=%s reason=idle terminated=%s",
            session_id[:8],
            transport is not None,
        )


# --------------------------------------------------------------------------- #
# SSE frame classification
# --------------------------------------------------------------------------- #
def is_useful_sse(body: bytes) -> bool:
    """True if an outbound SSE chunk carries a real message (not a keepalive).

    sse-starlette emits keepalive pings as comment lines (``: ping - <ts>``)
    which carry no ``data:``/``event:`` field. Only message frames count as
    useful work.
    """
    if not body:
        return False
    return b"data:" in body or b"event:" in body


# --------------------------------------------------------------------------- #
# Small ASGI helpers
# --------------------------------------------------------------------------- #
def _header(scope, name: bytes) -> Optional[str]:
    target = name.lower()
    for key, value in scope.get("headers") or []:
        if key.lower() == target:
            try:
                return value.decode("latin-1")
            except Exception:  # pragma: no cover - defensive
                return None
    return None


async def _send_status(send, status: int, headers: list[tuple[bytes, bytes]], body: bytes) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": headers + [(b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


# --------------------------------------------------------------------------- #
# Middleware
# --------------------------------------------------------------------------- #
class McpStreamGuardMiddleware:
    """ASGI middleware admitting and bounding /mcp requests."""

    def __init__(self, app, state: McpStreamGuardState) -> None:
        self.app = app
        self.state = state
        self.config = state.config

    # -- identity ------------------------------------------------------------ #
    @staticmethod
    def _bearer_token(scope) -> Optional[str]:
        auth = _header(scope, b"authorization")
        if auth and auth[:7].lower() == "bearer ":
            token = auth[7:].strip()
            return token or None
        return None

    @staticmethod
    def _is_standalone_sse_get(scope) -> bool:
        """Mirror FastMCP's full set of preconditions for establishing a NEW
        standalone GET SSE stream (mcp/server/streamable_http.py
        handle_get_request) before its duplicate (409) check. A GET only reaches
        that 409 — and thus is a genuine replacement worth tracking/superseding —
        when ALL of these hold; otherwise FastMCP 406/400/404s it or treats it as
        a replay, so the guard must pass it through untouched rather than cancel a
        healthy stream for a request the inner app will reject:

          * Accept includes text/event-stream (else 406).
          * No Last-Event-ID (else FastMCP replays events without creating a
            standalone stream).
          * Supported mcp-protocol-version (else 400). Absent header = default.

        Session-id validation (_validate_session) is covered implicitly: the
        duplicate supersede only fires when an existing tracked stream has the
        SAME session id, which is exactly what FastMCP's session check requires.
        """
        # Mirror FastMCP's _check_accept_headers exactly: comma-split, strip, then
        # a CASE-SENSITIVE startswith per token. A substring/lowercased match is
        # more permissive (e.g. "application/x-text/event-stream" or different
        # casing) and would supersede a stream FastMCP then 406s.
        accept_header = _header(scope, b"accept") or ""
        accept_types = [media_type.strip() for media_type in accept_header.split(",")]
        if not any(media_type.startswith("text/event-stream") for media_type in accept_types):
            return False
        if _header(scope, b"last-event-id"):
            return False
        if _SUPPORTED_PROTOCOL_VERSIONS is not None:
            version = _header(scope, b"mcp-protocol-version") or _DEFAULT_NEGOTIATED_VERSION
            if version not in _SUPPORTED_PROTOCOL_VERSIONS:
                return False
        return True

    def _identity_key(self, scope) -> str:
        # Only trust a client identity from a request the auth layer FULLY
        # accepted. This middleware runs after AuthenticationMiddleware, whose
        # BearerAuthBackend sets scope["user"].access_token only when the provider's
        # verify_token (incl. the OAuth-proxy JTI lookup + upstream token swap)
        # succeeded. A JWT that is merely signed/unexpired but rejected upstream
        # (revoked, missing JTI) leaves access None — we must NOT decode/budget it,
        # or an unauthenticated request could supersede a healthy stream before
        # RequireAuthMiddleware 401s it downstream.
        user = scope.get("user")
        access = getattr(user, "access_token", None)
        if access is not None:
            # Primary: the per-DCR MCP client_id from the FastMCP-issued JWT. On the
            # GoogleProvider OAuth-proxy path, access.client_id is the SHARED
            # upstream Google audience (identical for every client), which would
            # collapse all clients into one global budget. The raw bearer is the
            # FastMCP JWT, whose payload carries the real DCR client_id.
            issuer = self.state.jwt_issuer
            if issuer is not None:
                token = self._bearer_token(scope)
                if token:
                    try:
                        payload = issuer.verify_token(token)
                        client_id = payload.get("client_id")
                        if client_id:
                            return f"client:{client_id}"
                    except Exception:
                        pass  # fall through to the authenticated principal below
            client_id = getattr(access, "client_id", None)
            if client_id:
                return f"client:{client_id}"

        # Unauthenticated / local: key by edge IP + user agent.
        cf_ip = _header(scope, b"cf-connecting-ip")
        if not cf_ip:
            client = scope.get("client")
            cf_ip = client[0] if client else "unknown"
        ua = _header(scope, b"user-agent") or ""
        return f"anon:{cf_ip}|{ua}"

    def _is_mcp_path(self, scope) -> bool:
        path = (scope.get("path") or "").rstrip("/")
        return path == self.state.mcp_path

    # -- logging ------------------------------------------------------------- #
    def _log(
        self,
        event: str,
        *,
        scope=None,
        record: Optional[StreamRecord] = None,
        identity_key: Optional[str] = None,
        session_id: Optional[str] = None,
        reason: Optional[str] = None,
        age_ms: Optional[int] = None,
        active_get_count: Optional[int] = None,
        level: int = logging.INFO,
    ) -> None:
        parts = [event]
        ik = identity_key or (record.identity_key if record else None)
        if ik:
            parts.append(f"identity_hash={self.state.identity_hash(ik)}")
            if ik.startswith("client:"):
                parts.append(f"client_id_prefix={ik[len('client:'):][:8]}")
        if record is not None:
            parts.append(f"stream_id={record.stream_id}")
            if record.session_id:
                parts.append(f"session_id_prefix={record.session_id[:8]}")
        elif session_id:
            parts.append(f"session_id_prefix={session_id[:8]}")
        if reason:
            parts.append(f"reason={reason}")
        if age_ms is not None:
            parts.append(f"age_ms={age_ms}")
        if active_get_count is not None:
            parts.append(f"active_get_count={active_get_count}")
        if scope is not None:
            cf = _header(scope, b"cf-connecting-ip")
            if cf:
                parts.append(f"cf_connecting_ip={cf}")
            ua = _header(scope, b"user-agent")
            if ua:
                parts.append(f"user_agent={ua[:64]!r}")
        logger.log(level, " ".join(parts))

    # -- entrypoint ---------------------------------------------------------- #
    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or not self.config.enabled or not self._is_mcp_path(scope):
            await self.app(scope, receive, send)
            return

        method = scope.get("method")
        if method == "GET":
            await self._handle_get(scope, receive, send)
        elif method == "POST":
            await self._handle_post(scope, receive, send)
        elif method == "DELETE":
            await self._handle_delete(scope, receive, send)
        else:
            await self.app(scope, receive, send)

    # -- GET: bounded, budgeted SSE ----------------------------------------- #
    async def _handle_get(self, scope, receive, send):
        cfg = self.config
        identity_key = self._identity_key(scope)
        session_id = _header(scope, b"mcp-session-id")

        if cfg.get_mode == "post_only":
            await _send_status(
                send,
                405,
                [(b"allow", b"POST, DELETE"), (b"content-type", b"text/plain; charset=utf-8")],
                b"Method Not Allowed",
            )
            self._log(
                "MCP_STREAM_REJECT",
                scope=scope,
                identity_key=identity_key,
                session_id=session_id,
                reason="post_only",
            )
            return

        # Only manage GETs that will actually establish a standalone SSE stream.
        # Malformed GETs (no text/event-stream Accept) and event replays
        # (Last-Event-ID) are not standalone streams in FastMCP, so pass them
        # through untouched — admitting/superseding on them could cancel a
        # healthy stream for a request the inner app will 406 or replay.
        if not self._is_standalone_sse_get(scope):
            await self.app(scope, receive, send)
            return

        now = anyio.current_time()
        record = StreamRecord(
            stream_id=secrets.token_hex(8),
            identity_key=identity_key,
            session_id=session_id,
            started_at=now,
            last_useful=now,
            last_post=now,
        )

        admitted = await self._admit_get(record, scope)
        if not admitted:
            await _send_status(
                send,
                429,
                [(b"retry-after", b"1"), (b"content-type", b"text/plain; charset=utf-8")],
                b"Too Many Requests",
            )
            self._log(
                "MCP_STREAM_REJECT",
                scope=scope,
                record=record,
                reason="cap_supersede_timeout",
                level=logging.WARNING,
            )
            return

        active = await self.state.count_get_streams(identity_key)
        self._log("MCP_STREAM_OPEN", scope=scope, record=record, active_get_count=active)

        async def tracking_send(message):
            if message.get("type") == "http.response.body" and is_useful_sse(message.get("body", b"")):
                record.last_useful = anyio.current_time()
            await send(message)

        reason = "client_close"
        poll = max(
            0.02,
            min(
                0.5,
                cfg.work_idle_seconds / 4 if cfg.work_idle_seconds else 0.5,
                cfg.supersede_wait_seconds / 2 if cfg.supersede_wait_seconds else 0.5,
                cfg.max_age_seconds / 4 if cfg.max_age_seconds else 0.5,
            ),
        )
        try:
            async with anyio.create_task_group() as tg:

                async def watchdog():
                    nonlocal reason
                    while True:
                        await anyio.sleep(poll)
                        t = anyio.current_time()
                        if record.cancel_event.is_set():
                            reason = "superseded"
                            tg.cancel_scope.cancel()
                            return
                        # An in-flight POST on this stream's session is active
                        # work even though it produces no GET-channel frames, so
                        # do not close the stream as idle while one is processing.
                        idle = t - max(record.last_useful, record.last_post) >= cfg.work_idle_seconds
                        if idle and not self.state.has_inflight(record.session_id):
                            reason = "work_idle"
                            tg.cancel_scope.cancel()
                            return
                        if t - record.started_at >= cfg.max_age_seconds:
                            reason = "max_age"
                            tg.cancel_scope.cancel()
                            return

                tg.start_soon(watchdog)
                await self.app(scope, receive, tracking_send)
                tg.cancel_scope.cancel()  # app finished on its own; stop the watchdog
        finally:
            age_ms = int((anyio.current_time() - record.started_at) * 1000)
            async with self.state._lock:
                self.state.active_streams.pop(record.stream_id, None)
            self._log("MCP_STREAM_CLOSE", scope=scope, record=record, reason=reason, age_ms=age_ms)

    async def _admit_get(self, record: StreamRecord, scope) -> bool:
        """Admit a standalone GET stream, enforcing two invariants atomically:

          1. Per-session uniqueness — FastMCP allows only ONE standalone GET per
             session (a duplicate returns 409 and leaves the OLD stream alive, the
             cost-holding behavior this guard removes).
          2. Per-identity concurrency cap.

        Both the duplicate-session check AND the cap check are evaluated in the
        SAME locked section as the insertion, so concurrent reconnect bursts —
        whether same-session replacements or same-identity floods — cannot all
        observe room and insert past the invariant. When a duplicate or over-cap
        condition is found, the victim is superseded outside the lock and the loop
        re-checks under the lock before inserting.
        """
        cfg = self.config
        cap = cfg.max_get_streams_per_identity
        while True:
            async with self.state._lock:
                same_identity = [
                    r for r in self.state.active_streams.values()
                    if r.identity_key == record.identity_key
                ]
                dup = None
                if record.session_id:
                    dup = next(
                        (
                            r for r in same_identity
                            if r.session_id == record.session_id
                            and r.stream_id != record.stream_id
                        ),
                        None,
                    )
                if dup is None and (cap <= 0 or len(same_identity) < cap):
                    # No same-session duplicate and under the identity cap:
                    # reserve atomically under the lock that observed both.
                    self.state.active_streams[record.stream_id] = record
                    return True
                if dup is not None:
                    victim, duplicate = dup, True
                else:
                    victim, duplicate = min(same_identity, key=lambda r: r.started_at), False
                over_count = len(same_identity)

            if duplicate:
                self._log("MCP_STREAM_SUPERSEDE", scope=scope, record=record, reason="duplicate_session")
            else:
                self._log(
                    "MCP_STREAM_THRESHOLD",
                    scope=scope,
                    record=record,
                    reason="get_cap_exceeded",
                    active_get_count=over_count,
                    level=logging.WARNING,
                )
            victim.cancel_event.set()
            if not await self._wait_until_gone(victim.stream_id, cfg.supersede_wait_seconds):
                return False
            if not duplicate:
                self._log("MCP_STREAM_SUPERSEDE", scope=scope, record=record, reason="superseded_victim")

    async def _wait_until_gone(self, stream_id: str, timeout: float) -> bool:
        deadline = anyio.current_time() + max(0.0, timeout)
        while True:
            async with self.state._lock:
                if stream_id not in self.state.active_streams:
                    return True
            if anyio.current_time() >= deadline:
                return False
            await anyio.sleep(0.02)

    # -- POST: pass through, track session activity ------------------------- #
    async def _handle_post(self, scope, receive, send):
        req_session = _header(scope, b"mcp-session-id")
        captured = {"session_id": req_session, "status": None}
        inflight_keys: set = set()

        async def _mark_inflight(session_id):
            # Transient liveness signal (not persistent activity credit): while it
            # is set, the GET watchdog and reaper treat the session as active, so
            # a long-running tool call cannot have its correlated GET stream
            # closed or its session reaped mid-work. Cleared in finally regardless
            # of status, so a client looping fast rejected POSTs gains no credit.
            if session_id and session_id not in inflight_keys:
                inflight_keys.add(session_id)
                await self.state.inflight_inc(session_id)

        # Key in-flight against the request session id when known up front.
        await _mark_inflight(req_session)

        async def capturing_send(message):
            if message.get("type") == "http.response.start":
                status = message.get("status")
                captured["status"] = status
                for key, value in message.get("headers") or []:
                    if key.lower() == b"mcp-session-id":
                        try:
                            captured["session_id"] = value.decode("latin-1")
                        except Exception:  # pragma: no cover - defensive
                            pass
                # Persistent activity credit only for ACCEPTED POSTs, recorded as
                # soon as acceptance is known. Rejected POSTs (4xx/5xx) record no
                # activity — only the transient in-flight marker, which clears
                # when the request returns. Also key in-flight against the session
                # learned from the response header (e.g. an SSE POST that
                # establishes the session): a GET opened for that returned id
                # while the POST is still streaming must see it as in flight. The
                # mark happens before send() below, so the client cannot open that
                # GET before the in-flight marker exists.
                if status is not None and status < 400:
                    await _mark_inflight(captured["session_id"])
                    await self.state.touch_post(captured["session_id"], anyio.current_time())
            await send(message)

        try:
            await self.app(scope, receive, capturing_send)
        finally:
            # Finalize under a shield so cancellation (client disconnect on a long
            # streaming POST, shutdown) cannot abort these awaits and leak an
            # in-flight count non-zero forever. Refresh the accepted-activity
            # timestamp BEFORE decrementing in-flight so there is never a window
            # where an accepted session looks idle (inflight==0 and last_post
            # stale) to a concurrent reaper/watchdog.
            with anyio.CancelScope(shield=True):
                status = captured["status"]
                if status is not None and status < 400:
                    await self.state.touch_post(captured["session_id"], anyio.current_time())
                for session_id in inflight_keys:
                    await self.state.inflight_dec(session_id)

    # -- DELETE: pass through, drop tracked session ------------------------- #
    async def _handle_delete(self, scope, receive, send):
        session_id = _header(scope, b"mcp-session-id")
        try:
            await self.app(scope, receive, send)
        finally:
            await self.state.forget_session(session_id)


# --------------------------------------------------------------------------- #
# Binding helpers
# --------------------------------------------------------------------------- #
def _unwrap_session_manager(obj, depth: int = 6):
    """Walk an ASGI endpoint chain looking for a ``.session_manager`` attr."""
    seen = 0
    while obj is not None and seen < depth:
        manager = getattr(obj, "session_manager", None)
        if manager is not None:
            return manager
        obj = getattr(obj, "app", None)
        seen += 1
    return None


def bind_fastmcp_session_manager(app, state: McpStreamGuardState) -> bool:
    """Locate the FastMCP StreamableHTTPSessionManager on ``app`` and bind it to
    ``state`` so the reaper can terminate stale sessions. Returns True on success.

    The session manager is a local in ``create_streamable_http_app`` reachable
    only through the ``/mcp`` route's endpoint (``RequireAuthMiddleware`` ->
    ``StreamableHTTPASGIApp`` in auth mode, or the ASGI app directly otherwise).
    """
    routes = getattr(app, "routes", None) or []
    mcp_path = state.mcp_path

    candidates = []
    for route in routes:
        path = getattr(route, "path", None)
        endpoint = getattr(route, "app", None) or getattr(route, "endpoint", None)
        if path is None:
            continue
        if path.rstrip("/") == mcp_path:
            candidates.insert(0, endpoint)  # prefer the /mcp route
        else:
            candidates.append(endpoint)

    for endpoint in candidates:
        manager = _unwrap_session_manager(endpoint)
        if manager is not None:
            state._session_manager = manager
            logger.info("MCP_STREAM_GUARD bound session manager for reaping")
            return True

    logger.warning(
        "MCP_STREAM_GUARD could not locate StreamableHTTPSessionManager; "
        "session reaping will only clear local tracking"
    )
    return False


def bind_auth_jwt_issuer(app, state: McpStreamGuardState) -> bool:
    """Bind the FastMCP OAuth-proxy JWT issuer so the guard can decode the raw
    bearer JWT and budget by the per-DCR-client ``client_id``. Returns True on
    success. No-op when there is no auth provider (e.g. local mode) — identity
    then falls back to the authenticated principal or edge IP + user agent.

    The authenticated scope's ``access_token.client_id`` is the SHARED upstream
    audience on the GoogleProvider proxy path, so it cannot be used for
    per-client budgeting; the JWT ``client_id`` claim is the real DCR client id.
    """
    server = getattr(getattr(app, "state", None), "fastmcp_server", None)
    provider = getattr(server, "auth", None)
    if provider is None:
        logger.info("MCP_STREAM_GUARD no auth provider; budgeting by fallback identity")
        return False
    try:
        issuer = provider.jwt_issuer
    except Exception:
        logger.warning(
            "MCP_STREAM_GUARD auth provider exposes no JWT issuer; "
            "budgeting by fallback identity"
        )
        return False
    if issuer is None:
        return False
    state.jwt_issuer = issuer
    logger.info("MCP_STREAM_GUARD bound JWT issuer for per-client budgeting")
    return True


def install_reaper_lifespan(app, state: McpStreamGuardState) -> None:
    """Wrap the app lifespan so the session reaper runs for the app's lifetime."""
    router = getattr(app, "router", None)
    original = getattr(router, "lifespan_context", None)
    if router is None or original is None:
        logger.warning("MCP_STREAM_GUARD could not install reaper lifespan; reaper not started")
        return

    @contextlib.asynccontextmanager
    async def wrapped(app_):
        async with anyio.create_task_group() as tg:
            tg.start_soon(state.run_session_reaper)
            async with original(app_):
                yield
            tg.cancel_scope.cancel()

    router.lifespan_context = wrapped
