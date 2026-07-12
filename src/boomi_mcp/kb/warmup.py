"""Deferred, thread-safe warmup of the Boomi docs KB.

The heavy KB build (Chroma open + embedding-model load) is the dominant
cold-start cost. Running it at module import blocks uvicorn's socket bind, so a
cold Cloud Run instance accepts zero connections until it finishes and the
client times out. ``KbWarmup`` moves that build off the import path into a
single daemon thread and gives MCP tool calls one atomic ``resolve()``
operation: a ready service, or a bounded structured response
(``warming_up`` / ``kb_unavailable``) — never a hang.

Design notes:

* ``threading`` primitives (NOT asyncio) — FastMCP runs the sync ``def`` KB
  tools in worker threads via ``anyio.to_thread``, so ``resolve()`` is called
  from a thread, never the event loop.
* Single in-flight build, guarded by a lock; ``kick()`` is idempotent so racing
  callers (the eager first-request hook and the first tool call) never start two
  builds.
* ``resolve()`` admits at most ``max_waiters`` long waiters while WARMING; the
  bounded wait holds a request in flight (CPU allocated on request-billed Cloud
  Run) so the build advances, while overflow callers return ``warming_up`` in
  well under a second instead of starving the tool-worker thread pool. After a
  wait, the state is inspected and the result constructed under ONE lock
  acquisition, so readiness can never race with response construction.
* State machine ``idle -> warming -> ready | failed`` with self-heal: a failed
  build re-attempts after a cooldown on the next ``resolve()`` (a transient
  OOM/disk failure recovers without an instance restart). A failed build
  signals all admitted waiters immediately.
* No detail leak: client responses carry a generic message + coarse error_type;
  the full exception detail is logged server-side only.
"""
import logging
import math
import threading
import time
from dataclasses import dataclass

logger = logging.getLogger("boomi.kb.warmup")

# State machine values.
IDLE = "idle"
WARMING = "warming"
READY = "ready"
FAILED = "failed"

# warming_up retry-hint clamps (seconds): the hint is derived from the
# remaining share of the expected build duration and clamped into this window,
# so clients neither hammer a build that has just started nor sleep past one
# that is about to finish. When the estimate is already exceeded, hint the
# floor. The kb_unavailable hint is NOT a constant: it is derived from the
# REMAINING retry cooldown (see _kb_unavailable_locked).
WARMING_UP_RETRY_MIN = 5
WARMING_UP_RETRY_MAX = 60

# Production defaults (also pinned in cloudbuild.yaml): a bounded wait just
# above the measured p95+max build time (2026-07-05..2026-07-12 window: mean
# 48.6s, p95 58.4s, max 62.8s), the expected duration for retry hints, and the
# long-waiter admission cap.
DEFAULT_WAIT_SECONDS = 65.0
DEFAULT_EXPECTED_SECONDS = 60.0
DEFAULT_MAX_WAITERS = 4

# Build-running-longer-than this logs a STUCK warning (detection only — Python
# cannot force-kill the thread; the bounded resolve() wait already prevents any
# client hang, so a stuck build degrades to persistent warming_up, not a hang).
DEFAULT_STUCK_AFTER_SECONDS = 120.0


@dataclass(frozen=True)
class KbResolution:
    """Atomic result of one resolve(): a ready service XOR a client response."""

    service: object
    response: dict

    @property
    def ready(self):
        return self.service is not None


class KbWarmup:
    """Owns the deferred heavy KB build and serves atomic readiness resolution."""

    def __init__(
        self,
        bootstrap,
        builder=None,
        retry_cooldown_seconds=30.0,
        stuck_after_seconds=DEFAULT_STUCK_AFTER_SECONDS,
        time_fn=time.monotonic,
        wait_seconds=DEFAULT_WAIT_SECONDS,
        expected_seconds=DEFAULT_EXPECTED_SECONDS,
        max_waiters=DEFAULT_MAX_WAITERS,
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
        self._wait_seconds = float(wait_seconds)
        self._expected_seconds = float(expected_seconds)
        self._max_waiters = int(max_waiters)

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
        self._waiters = 0
        self._admitted_total = 0
        self._overflow_total = 0

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

    def resolve(self):
        """Atomically resolve readiness: kick, optionally wait, return a result.

        READY returns the service immediately; FAILED returns a snapshotted
        ``kb_unavailable`` response immediately. While WARMING, at most
        ``max_waiters`` callers are admitted (under the state lock) to a long
        wait on the readiness event, bounded by ``wait_seconds``; every
        admitted caller releases its slot in ``finally``. Overflow callers
        return ``warming_up`` immediately without occupying a slot. After a
        wait, the state is inspected and the result constructed under the SAME
        lock acquisition, so a build that finished right at the timeout is
        served, never mis-reported as warming.
        """
        self.kick()
        with self._lock:
            if self._state == READY:
                return KbResolution(self._service, None)
            if self._state == FAILED:
                return KbResolution(None, self._kb_unavailable_locked())
            # WARMING (IDLE is unreachable here: kick() either started a build
            # or left FAILED in place). Admit or overflow.
            if self._waiters >= self._max_waiters:
                self._overflow_total += 1
                logger.info(
                    "KB_RESOLVE_OVERFLOW overflow_total=%d", self._overflow_total
                )
                return KbResolution(None, self._warming_up_locked())
            self._waiters += 1
            self._admitted_total += 1
            logger.info(
                "KB_RESOLVE_ADMITTED waiters=%d admitted_total=%d",
                self._waiters, self._admitted_total,
            )
            done = self._done

        try:
            done.wait(timeout=max(0.0, self._wait_seconds))
        finally:
            with self._lock:
                self._waiters -= 1

        with self._lock:
            if self._state == READY:
                return KbResolution(self._service, None)
            if self._state == FAILED:
                return KbResolution(None, self._kb_unavailable_locked())
            self._maybe_log_stuck_locked()
            return KbResolution(None, self._warming_up_locked())

    # -- internals ---------------------------------------------------------- #
    def _warming_up_locked(self):
        """warming_up response with a remaining-estimate retry hint.

        Caller must hold the lock. ``remaining = max(0, expected - elapsed)``;
        while positive the hint is ceil(remaining) clamped into
        [WARMING_UP_RETRY_MIN, WARMING_UP_RETRY_MAX]; once the estimate is
        exceeded the hint drops to the floor (short polls — the build should
        finish any moment or be declared stuck).
        """
        elapsed = self._time() - (self._started_at if self._started_at is not None
                                  else self._time())
        remaining = max(0.0, self._expected_seconds - elapsed)
        if remaining > 0:
            retry_after = min(WARMING_UP_RETRY_MAX,
                              max(WARMING_UP_RETRY_MIN, math.ceil(remaining)))
        else:
            retry_after = WARMING_UP_RETRY_MIN
        logger.info("KB_RESPONSE status=warming_up retry_after=%d", retry_after)
        return {
            "_success": False,
            "error": "warming_up",
            "message": "Boomi Docs KB is still loading. Retry shortly.",
            "retry_after_seconds": retry_after,
        }

    def _kb_unavailable_locked(self):
        """kb_unavailable response for the FAILED state (read under lock).

        Carries no raw exception detail — only a coarse error_type category.
        """
        error_type = self._error_type
        retry_after = self._remaining_cooldown_locked()
        logger.info("KB_RESPONSE status=kb_unavailable error_type=%s", error_type)
        return {
            "_success": False,
            "error": "kb_unavailable",
            "message": "Boomi Docs KB is temporarily unavailable. Please retry shortly.",
            "error_type": error_type or "KbStartupError",
            # Soonest a retry could change state = when the cooldown lets the
            # next resolve() re-kick. Tracks the configured cooldown, not a
            # fixed default, so clients don't retry into kb_unavailable.
            "retry_after_seconds": retry_after,
        }

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
            admitted_total = self._admitted_total
            overflow_total = self._overflow_total
        # A failed build signals all admitted waiters immediately, same as a
        # successful one — nobody sleeps out the full wait against a known
        # terminal state.
        done.set()
        elapsed = self._time() - start
        outcome = "ready" if error is None else "failed"
        logger.info(
            "KB_WARMUP_ELAPSED seconds=%.2f outcome=%s admitted_total=%d "
            "overflow_total=%d",
            elapsed, outcome, admitted_total, overflow_total,
        )
        if error is None:
            logger.info("KB_WARMUP_SUCCEEDED seconds=%.2f", elapsed)
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
