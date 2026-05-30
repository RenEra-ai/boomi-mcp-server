"""Deferred, thread-safe warmup of the Boomi docs KB.

The heavy KB build (Chroma open + embedding-model load) is the dominant
cold-start cost. Running it at module import blocks uvicorn's socket bind, so a
cold Cloud Run instance accepts zero connections until it finishes and the
client times out. ``KbWarmup`` moves that build off the import path into a
single daemon thread and lets MCP tool calls return a bounded structured
response (``warming_up``) instead of hanging.

Design notes:

* ``threading`` primitives (NOT asyncio) — FastMCP runs the sync ``def`` KB
  tools in worker threads via ``anyio.to_thread``, so ``get()`` is called from a
  thread, never the event loop.
* Single in-flight build, guarded by a lock; ``kick()`` is idempotent so racing
  callers (the eager first-request hook and the first tool call) never start two
  builds.
* State machine ``idle -> warming -> ready | failed`` with self-heal: a failed
  build re-attempts after a cooldown on the next ``get()`` (a transient OOM/disk
  failure recovers without an instance restart).
* No detail leak: client responses carry a generic message + coarse error_type;
  the full exception detail is logged server-side only.
"""
import logging
import math
import threading
import time

logger = logging.getLogger("boomi.kb.warmup")

# State machine values.
IDLE = "idle"
WARMING = "warming"
READY = "ready"
FAILED = "failed"

# Client-facing retry hint for warming_up (seconds) — matches the default
# in-request wait; a short poll interval while the build is in flight. The
# kb_unavailable hint is NOT a constant: it is derived from the REMAINING retry
# cooldown (see not_ready_response), so a tuned BOOMI_DOCS_WARMUP_RETRY_COOLDOWN
# never leaves clients retrying before a re-kick is even possible.
WARMING_UP_RETRY_AFTER = 5

# Build-running-longer-than this logs a STUCK warning (detection only — Python
# cannot force-kill the thread; the bounded get() wait already prevents any
# client hang, so a stuck build degrades to persistent warming_up, not a hang).
DEFAULT_STUCK_AFTER_SECONDS = 120.0


class KbWarmup:
    """Owns the deferred heavy KB build and serves a state-based readiness view."""

    def __init__(
        self,
        bootstrap,
        builder=None,
        retry_cooldown_seconds=30.0,
        stuck_after_seconds=DEFAULT_STUCK_AFTER_SECONDS,
        time_fn=time.monotonic,
    ):
        self._bootstrap = bootstrap
        if builder is None:
            # Imported lazily so this module stays ML-free at import; importing
            # the function object does not import chromadb (that happens inside
            # build_kb_service_heavy's body).
            from .service import build_kb_service_heavy

            builder = build_kb_service_heavy
        self._builder = builder
        self._retry_cooldown = float(retry_cooldown_seconds)
        self._stuck_after = float(stuck_after_seconds)
        self._time = time_fn

        self._lock = threading.Lock()
        self._done = threading.Event()
        self._state = IDLE
        self._service = None
        self._error = None
        self._error_type = None
        self._failed_at = None
        self._started_at = None
        self._stuck_logged = False
        self._thread = None

    # -- public API --------------------------------------------------------- #
    def kick(self):
        """Idempotently ensure a single build is running.

        No-op while a build is in flight (WARMING) or already done (READY).
        From FAILED, restarts only after the retry cooldown has elapsed
        (self-heal). Safe to call concurrently from many worker threads — the
        lock guarantees at most one in-flight build.
        """
        with self._lock:
            if self._state in (WARMING, READY):
                return
            is_retry = False
            if self._state == FAILED:
                elapsed = self._time() - (self._failed_at or 0.0)
                if elapsed < self._retry_cooldown:
                    return
                is_retry = True

            # idle, or failed + cooldown elapsed -> (re)start a fresh build.
            self._state = WARMING
            self._service = None
            self._error = None
            self._error_type = None
            self._failed_at = None
            self._started_at = self._time()
            self._stuck_logged = False
            self._done = threading.Event()
            done = self._done
            thread = threading.Thread(
                target=self._run, args=(done,), name="kb-warmup", daemon=True
            )
            self._thread = thread
            logger.info("KB_WARMUP_RETRY" if is_retry else "KB_WARMUP_STARTED")
        thread.start()

    def get(self, wait_seconds):
        """Return the ready KbService, or None if not ready within wait_seconds.

        Always kicks first so warmup proceeds even when the eager hook is off /
        never fired (otherwise an instance could report warming_up forever). The
        bounded wait holds a request in flight (CPU allocated on request-billed
        Cloud Run) so the build advances. Returns None for warming AND failed —
        the caller renders not_ready_response() for the exact client status.
        """
        self.kick()
        with self._lock:
            if self._state == READY:
                return self._service
            done = self._done
        done.wait(timeout=max(0.0, float(wait_seconds)))
        with self._lock:
            if self._state == READY:
                return self._service
            self._maybe_log_stuck_locked()
            return None

    def not_ready_response(self):
        """Structured client response for a not-ready state (read under lock).

        Keyed off the CURRENT state, never error history: a cooldown re-kick
        (state back to WARMING) reports warming_up, not stale kb_unavailable.
        Carries no raw exception detail — only a coarse error_type category.
        """
        with self._lock:
            state = self._state
            error_type = self._error_type
            retry_after = self._remaining_cooldown_locked()
        if state == FAILED:
            logger.info("KB_RESPONSE status=kb_unavailable error_type=%s", error_type)
            return {
                "_success": False,
                "error": "kb_unavailable",
                "message": "Boomi Docs KB is temporarily unavailable. Please retry shortly.",
                "error_type": error_type or "KbStartupError",
                # Soonest a retry could change state = when the cooldown lets the
                # next get() re-kick. Tracks the configured cooldown, not a fixed
                # default, so clients don't retry into kb_unavailable.
                "retry_after_seconds": retry_after,
            }
        # IDLE or WARMING (incl. a retry build in flight) -> still loading.
        logger.info("KB_RESPONSE status=warming_up")
        return {
            "_success": False,
            "error": "warming_up",
            "message": "Boomi Docs KB is still loading. Retry shortly.",
            "retry_after_seconds": WARMING_UP_RETRY_AFTER,
        }

    # -- internals ---------------------------------------------------------- #
    def _run(self, done):
        start = self._time()
        service = None
        error = None
        try:
            service = self._builder(self._bootstrap)
        except Exception as e:  # noqa: BLE001 — record, never crash the thread
            error = e
        # Set the terminal state under the lock BEFORE signalling done, so a
        # waiter woken by done.set() always reads the final state (no transient
        # warming_up after a successful build).
        with self._lock:
            if self._done is done:  # ignore a superseded build's result
                if error is None:
                    self._state = READY
                    self._service = service
                    self._error = None
                    self._error_type = None
                else:
                    self._state = FAILED
                    self._service = None
                    self._error = str(error)
                    self._error_type = type(error).__name__
                    self._failed_at = self._time()
        done.set()
        if error is None:
            logger.info("KB_WARMUP_SUCCEEDED seconds=%.2f", self._time() - start)
        else:
            logger.error(
                "KB_WARMUP_FAILED error_type=%s detail=%s",
                type(error).__name__,
                error,
            )

    def _remaining_cooldown_locked(self):
        """Seconds until a failed build may re-attempt, as an int >= 1.

        Caller must hold the lock. Returns the remaining slice of the configured
        retry cooldown since the last failure (or the full cooldown right after a
        failure); clamped to >= 1 so the client always gets a positive retry hint
        and an already-elapsed cooldown reads as "retry now".

        Rounds UP (ceil): this is the soonest a retry can CHANGE state, so a hint
        that rounded a fractional remainder down (e.g. 2.4 -> 2) would have the
        client retry before kick()'s cooldown elapses and get another
        kb_unavailable. Ceiling lands the retry at or just past the boundary.
        """
        remaining = self._retry_cooldown - (self._time() - (self._failed_at or 0.0))
        return max(1, math.ceil(remaining))

    def _maybe_log_stuck_locked(self):
        """Log once if the in-flight build has exceeded the stuck threshold.

        Caller must hold the lock. Detection only (see module docstring)."""
        if (
            self._state == WARMING
            and not self._stuck_logged
            and self._started_at is not None
            and (self._time() - self._started_at) >= self._stuck_after
        ):
            self._stuck_logged = True
            logger.warning(
                "KB_WARMUP_STUCK seconds=%.2f", self._time() - self._started_at
            )
