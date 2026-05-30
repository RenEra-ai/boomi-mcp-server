"""Unit tests for KbWarmup — the deferred, thread-safe KB build state machine.

Uses INJECTED fake builders only, so this module needs NO ML deps (no
importorskip) and exercises the warmup logic deterministically: state-based
responses, no-leak sanitization, get()-triggers-kick, single concurrent build,
cooldown self-heal, and the daemon worker.
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
from boomi_mcp.kb.warmup import WARMING_UP_RETRY_AFTER, KbWarmup


# --- warming_up -> ready ------------------------------------------------------

def test_warming_up_then_ready():
    gate = threading.Event()
    result = object()

    def builder(_bootstrap):
        gate.wait(timeout=5)
        return result

    w = KbWarmup(bootstrap=None, builder=builder)

    # Build is in flight (blocked on the gate): a short wait yields None +
    # the exact warming_up shape.
    assert w.get(wait_seconds=0.1) is None
    resp = w.not_ready_response()
    assert resp == {
        "_success": False,
        "error": "warming_up",
        "message": "Boomi Docs KB is still loading. Retry shortly.",
        "retry_after_seconds": WARMING_UP_RETRY_AFTER,
    }

    gate.set()
    assert w.get(wait_seconds=5) is result


# --- failed build -> sanitized kb_unavailable --------------------------------

def test_failed_build_returns_sanitized_kb_unavailable():
    def builder(_bootstrap):
        raise KbStartupError("BOOMI_DOCS_DB_PATH does not exist: /app/kb/secret")

    w = KbWarmup(bootstrap=None, builder=builder, retry_cooldown_seconds=30.0)
    assert w.get(wait_seconds=5) is None
    resp = w.not_ready_response()
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
    )
    assert w.get(wait_seconds=5) is None
    # Right after the failure: the full 300s cooldown, not 30.
    assert w.not_ready_response()["retry_after_seconds"] == 300
    # Halfway through the cooldown: the REMAINING time.
    clock["t"] = 1150.0
    assert w.not_ready_response()["retry_after_seconds"] == 150
    # Past the cooldown: clamped to "retry now" (>= 1).
    clock["t"] = 2000.0
    assert w.not_ready_response()["retry_after_seconds"] == 1


# --- get() triggers kick() ----------------------------------------------------

def test_get_triggers_build_without_explicit_kick():
    calls = []
    result = object()

    def builder(_bootstrap):
        calls.append(1)
        return result

    w = KbWarmup(bootstrap=None, builder=builder)
    # No explicit kick(): get() must start the build itself (else an instance
    # with eager off would return warming_up forever).
    assert w.get(wait_seconds=5) is result
    assert len(calls) == 1


# --- one build across concurrent get()s --------------------------------------

def test_concurrent_get_shares_single_build():
    gate = threading.Event()
    calls = []
    result = object()

    def builder(_bootstrap):
        calls.append(1)
        gate.wait(timeout=5)
        return result

    w = KbWarmup(bootstrap=None, builder=builder)
    results = []

    def worker():
        results.append(w.get(wait_seconds=5))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    time.sleep(0.2)  # let all callers reach the wait
    gate.set()
    for t in threads:
        t.join(timeout=5)

    assert len(calls) == 1, "expected a single in-flight build across concurrent get()"
    assert results and all(r is result for r in results)


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
    )

    # First build fails -> kb_unavailable.
    assert w.get(wait_seconds=5) is None
    assert w.not_ready_response()["error"] == "kb_unavailable"

    # Advance past the cooldown; get() re-kicks a retry that is in flight.
    clock["t"] += 31
    assert w.get(wait_seconds=0.1) is None
    # MUST be warming_up (state-based), NOT a stale kb_unavailable from _error.
    assert w.not_ready_response()["error"] == "warming_up"

    gate2.set()
    assert w.get(wait_seconds=5) is result


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
    )

    # First attempt fails.
    assert w.get(wait_seconds=5) is None
    # Within cooldown: kick() is a no-op, no second build.
    assert w.get(wait_seconds=5) is None
    assert state["attempt"] == 1
    # Past cooldown: the next get() re-attempts and succeeds.
    clock["t"] = 31.0
    assert w.get(wait_seconds=5) is result
    assert state["attempt"] == 2
