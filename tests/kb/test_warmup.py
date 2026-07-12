"""Unit tests for KbWarmup — the deferred, thread-safe KB build state machine.

Uses INJECTED fake builders only, so this module needs NO ML deps (no
importorskip) and exercises the warmup logic deterministically: the atomic
resolve() contract, formula-driven retry hints, no-leak sanitization,
resolve()-triggers-kick, single concurrent build, cooldown self-heal, and the
daemon worker.
"""
import os
import sys
import threading
import time
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = str(Path(_HERE).parents[1])
_SRC = os.path.join(_ROOT, "src")
for _p in (_HERE, _SRC, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.kb.errors import KbStartupError
from boomi_mcp.kb.warmup import (
    WARMING_UP_RETRY_MAX,
    WARMING_UP_RETRY_MIN,
    KbWarmup,
)


# --- warming_up -> ready ------------------------------------------------------

def test_warming_up_then_ready():
    gate = threading.Event()
    result = object()

    def builder(_bootstrap):
        gate.wait(timeout=5)
        return result

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=0.05)

    # Build is in flight (blocked on the gate): a short admitted wait times out
    # and yields the exact warming_up shape. Elapsed is ~0, so the hint is the
    # full expected duration (60), inside the [5, 60] clamp.
    res = w.resolve()
    assert not res.ready
    assert res.service is None
    assert res.response == {
        "_success": False,
        "error": "warming_up",
        "message": "Boomi Docs KB is still loading. Retry shortly.",
        "retry_after_seconds": 60,
    }

    gate.set()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        res = w.resolve()
        if res.ready:
            break
    assert res.ready
    assert res.service is result
    assert res.response is None


def test_warming_up_retry_hint_follows_remaining_estimate():
    """remaining = max(0, expected - elapsed); positive -> clamp(ceil, 5, 60);
    exceeded -> 5."""
    clock = {"t": 1000.0}
    gate = threading.Event()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        return object()

    w = KbWarmup(
        bootstrap=None, builder=builder, wait_seconds=0.0,
        expected_seconds=60.0, time_fn=lambda: clock["t"],
    )
    try:
        # elapsed 0 -> remaining 60 -> ceil 60 -> hint 60 (max clamp boundary).
        assert w.resolve().response["retry_after_seconds"] == 60
        # elapsed 30 -> remaining 30 -> hint 30.
        clock["t"] = 1030.0
        assert w.resolve().response["retry_after_seconds"] == 30
        # elapsed 57.5 -> remaining 2.5 -> ceil 3 -> floor clamp to 5.
        clock["t"] = 1057.5
        assert w.resolve().response["retry_after_seconds"] == 5
        # elapsed 61 -> remaining 0 (estimate exceeded) -> 5.
        clock["t"] = 1061.0
        assert w.resolve().response["retry_after_seconds"] == 5
        assert WARMING_UP_RETRY_MIN == 5 and WARMING_UP_RETRY_MAX == 60
    finally:
        gate.set()


# --- failed build -> sanitized kb_unavailable --------------------------------

def test_failed_build_returns_sanitized_kb_unavailable():
    def builder(_bootstrap):
        raise KbStartupError("BOOMI_DOCS_DB_PATH does not exist: /app/kb/secret")

    w = KbWarmup(bootstrap=None, builder=builder, retry_cooldown_seconds=30.0,
                 wait_seconds=5.0)
    res = w.resolve()
    assert not res.ready
    resp = res.response
    assert resp["error"] == "kb_unavailable"
    assert resp["error_type"] == "KbStartupError"
    # retry hint is derived from the (default 30s) cooldown — a positive int no
    # larger than the configured cooldown.
    assert isinstance(resp["retry_after_seconds"], int)
    assert 1 <= resp["retry_after_seconds"] <= 30
    assert resp["message"] == (
        "Boomi Docs KB is temporarily unavailable. Please retry shortly."
    )
    # No raw exception detail / internal path leaks into the client response.
    blob = repr(resp)
    assert "/app/kb/secret" not in blob
    assert "BOOMI_DOCS_DB_PATH" not in blob


def test_kb_unavailable_retry_after_tracks_configured_cooldown():
    """The failed-state retry hint must reflect the configured cooldown (and
    shrink as it elapses), not a hard-coded default — otherwise a tuned-up
    BOOMI_DOCS_WARMUP_RETRY_COOLDOWN leaves clients retrying into kb_unavailable
    before a re-kick is even possible."""
    clock = {"t": 1000.0}

    def builder(_bootstrap):
        raise KbStartupError("transient")

    w = KbWarmup(
        bootstrap=None,
        builder=builder,
        retry_cooldown_seconds=300.0,
        time_fn=lambda: clock["t"],
        wait_seconds=5.0,
    )
    assert w.resolve().response["error"] == "kb_unavailable"
    # Right after the failure: the full 300s cooldown, not 30.
    assert w.resolve().response["retry_after_seconds"] == 300
    # Halfway through the cooldown: the REMAINING time.
    clock["t"] = 1150.0
    assert w.resolve().response["retry_after_seconds"] == 150
    # Past the cooldown: resolve() re-kicks; the builder fails again and the
    # fresh failure re-arms the full cooldown.
    clock["t"] = 2000.0
    assert w.resolve().response["retry_after_seconds"] == 300


def test_admitted_waiter_wakes_immediately_on_build_failure():
    """A failed build must signal all admitted waiters at once — an admitted
    caller with a long wait budget returns kb_unavailable as soon as the
    builder raises, not after the full wait."""
    gate = threading.Event()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        raise KbStartupError("boom")

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=30.0)
    results = []

    def worker():
        results.append(w.resolve())

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.1)  # let the worker get admitted and start waiting
    start = time.monotonic()
    gate.set()
    t.join(timeout=5)
    assert not t.is_alive()
    assert time.monotonic() - start < 1.0, "waiter did not wake immediately"
    assert results and results[0].response["error"] == "kb_unavailable"


# --- resolve() triggers kick() -------------------------------------------------

def test_resolve_triggers_build_without_explicit_kick():
    calls = []
    result = object()

    def builder(_bootstrap):
        calls.append(1)
        return result

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=5.0)
    # No explicit kick(): resolve() must start the build itself (else an
    # instance with eager off would return warming_up forever).
    assert w.resolve().service is result
    assert len(calls) == 1


# --- one build across concurrent resolves --------------------------------------

def test_concurrent_resolve_shares_single_build():
    gate = threading.Event()
    calls = []
    result = object()

    def builder(_bootstrap):
        calls.append(1)
        gate.wait(timeout=5)
        return result

    # max_waiters=8 so every caller is admitted (single-build is what is under
    # test here, not admission).
    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=5.0, max_waiters=8)
    results = []

    def worker():
        results.append(w.resolve())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    time.sleep(0.2)  # let all callers reach the wait
    gate.set()
    for t in threads:
        t.join(timeout=5)

    assert len(calls) == 1, "expected a single in-flight build across concurrent resolve()"
    assert results and all(r.service is result for r in results)


# --- atomic readiness at the timeout boundary ----------------------------------

def test_resolve_serves_build_that_finishes_exactly_at_wait_timeout():
    """The state inspection and result construction happen under ONE lock after
    the wait — a build that completes precisely when the waiter's wait times
    out must be SERVED, never mis-reported as warming_up. Simulated by patching
    the admitted waiter's event.wait to complete the build, wait for the
    terminal state, and then return False as if it had timed out."""
    gate = threading.Event()
    result = object()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        return result

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=0.2)
    w.kick()
    with w._lock:
        done = w._done
    orig_wait = done.wait

    def timeout_racing_wait(timeout=None):
        gate.set()          # let the build finish now
        orig_wait(5)        # wait until _run set the terminal state + done
        return False        # ...but report "timed out" to the caller

    done.wait = timeout_racing_wait
    try:
        res = w.resolve()
    finally:
        done.wait = orig_wait
    assert res.ready, "a build finishing at the timeout boundary must be served"
    assert res.service is result


# --- daemon worker ------------------------------------------------------------

def test_worker_thread_is_daemon():
    gate = threading.Event()

    def builder(_bootstrap):
        gate.wait(timeout=5)
        return object()

    w = KbWarmup(bootstrap=None, builder=builder)
    w.kick()
    try:
        assert w._thread is not None
        assert w._thread.daemon is True
    finally:
        gate.set()
        w._thread.join(timeout=5)


# --- state-based response: retry in flight is warming_up, not stale failure ---

def test_retry_in_flight_reports_warming_up_not_stale_kb_unavailable():
    clock = {"t": 1000.0}
    gate2 = threading.Event()
    state = {"attempt": 0}
    result = object()

    def builder(_bootstrap):
        state["attempt"] += 1
        if state["attempt"] == 1:
            raise KbStartupError("transient")
        gate2.wait(timeout=5)  # the retry build blocks so we can observe WARMING
        return result

    w = KbWarmup(
        bootstrap=None,
        builder=builder,
        retry_cooldown_seconds=30.0,
        time_fn=lambda: clock["t"],
        wait_seconds=0.05,
    )

    # First build fails -> kb_unavailable.
    assert w.resolve().response["error"] == "kb_unavailable"

    # Advance past the cooldown; resolve() re-kicks a retry that is in flight.
    clock["t"] += 31
    # MUST be warming_up (state-based), NOT a stale kb_unavailable from _error.
    assert w.resolve().response["error"] == "warming_up"

    gate2.set()
    deadline = time.monotonic() + 5
    res = w.resolve()
    while not res.ready and time.monotonic() < deadline:
        res = w.resolve()
    assert res.service is result


# --- self-heal after cooldown -------------------------------------------------

def test_failed_build_self_heals_after_cooldown():
    clock = {"t": 0.0}
    state = {"attempt": 0}
    result = object()

    def builder(_bootstrap):
        state["attempt"] += 1
        if state["attempt"] == 1:
            raise KbStartupError("transient")
        return result

    w = KbWarmup(
        bootstrap=None,
        builder=builder,
        retry_cooldown_seconds=30.0,
        time_fn=lambda: clock["t"],
        wait_seconds=5.0,
    )

    # First attempt fails.
    assert w.resolve().response["error"] == "kb_unavailable"
    # Within cooldown: kick() is a no-op, no second build.
    assert w.resolve().response["error"] == "kb_unavailable"
    assert state["attempt"] == 1
    # Past cooldown: the next resolve() re-attempts and succeeds.
    clock["t"] = 31.0
    assert w.resolve().service is result
    assert state["attempt"] == 2
