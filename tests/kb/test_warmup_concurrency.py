"""Concurrency tests for KbWarmup.resolve() waiter admission.

Injected gated builders only — no ML deps. Covers the plan's admission
contract: at most max_waiters long waiters while WARMING, overflow callers
return warming_up in well under a second without occupying a slot, an
unrelated synchronous call is never starved, and every admitted caller wakes
on success AND on failure.
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
from boomi_mcp.kb.warmup import KbWarmup


def _run_threads(n, fn):
    threads = [threading.Thread(target=fn) for _ in range(n)]
    for t in threads:
        t.start()
    return threads


def _wait_for_admitted(w, expected, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with w._lock:
            if w._waiters >= expected:
                return
        time.sleep(0.005)
    raise AssertionError(f"never reached {expected} admitted waiters")


def test_admits_at_most_max_waiters_and_overflows_fast():
    gate = threading.Event()
    result = object()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        return result

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=5.0, max_waiters=4)
    outcomes = []
    lock = threading.Lock()

    def worker():
        start = time.monotonic()
        res = w.resolve()
        with lock:
            outcomes.append((res, time.monotonic() - start))

    threads = _run_threads(8, worker)
    try:
        _wait_for_admitted(w, 4)
        # The 4 non-admitted callers overflow quickly with warming_up.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with lock:
                if len(outcomes) == 4:
                    break
            time.sleep(0.005)
        with lock:
            overflow = list(outcomes)
        assert len(overflow) == 4, f"expected 4 overflow returns, got {len(overflow)}"
        for res, elapsed in overflow:
            assert res.response["error"] == "warming_up"
            assert elapsed < 1.0, f"overflow caller took {elapsed:.3f}s"
        # Admission never exceeded the cap.
        with w._lock:
            assert w._waiters == 4
            assert w._overflow_total == 4
    finally:
        gate.set()
        for t in threads:
            t.join(timeout=5)

    assert len(outcomes) == 8
    served = [res for res, _ in outcomes if res.ready]
    assert len(served) == 4
    assert all(res.service is result for res in served)


def test_burst_of_40_calls_does_not_starve_unrelated_work_success_case():
    """40 simultaneous docs calls + one unrelated synchronous tool: <= 4 long
    waiters, every overflow under one second, the unrelated call under two
    seconds, and all admitted callers wake with the service on success."""
    gate = threading.Event()
    result = object()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        return result

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=5.0, max_waiters=4)
    outcomes = []
    lock = threading.Lock()
    peak = {"waiters": 0}

    def worker():
        start = time.monotonic()
        res = w.resolve()
        with lock:
            outcomes.append((res, time.monotonic() - start))

    unrelated = {}

    def unrelated_tool():
        start = time.monotonic()
        # Stand-in for any non-KB synchronous tool: pure computation.
        unrelated["result"] = sum(range(10_000))
        unrelated["elapsed"] = time.monotonic() - start

    threads = _run_threads(40, worker)
    sampler_stop = threading.Event()

    def sampler():
        while not sampler_stop.is_set():
            with w._lock:
                peak["waiters"] = max(peak["waiters"], w._waiters)
            time.sleep(0.002)

    sampler_thread = threading.Thread(target=sampler)
    sampler_thread.start()
    unrelated_thread = threading.Thread(target=unrelated_tool)
    unrelated_thread.start()
    try:
        unrelated_thread.join(timeout=5)
        assert unrelated["elapsed"] < 2.0

        # All 36 overflow callers return well under a second each.
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            with lock:
                if len(outcomes) == 36:
                    break
            time.sleep(0.005)
        with lock:
            overflow = list(outcomes)
        assert len(overflow) == 36
        for res, elapsed in overflow:
            assert res.response["error"] == "warming_up"
            assert elapsed < 1.0
    finally:
        gate.set()
        for t in threads:
            t.join(timeout=5)
        sampler_stop.set()
        sampler_thread.join(timeout=5)

    assert peak["waiters"] <= 4, f"admission cap exceeded: {peak['waiters']}"
    served = [res for res, _ in outcomes if res.ready]
    assert len(served) == 4
    assert all(res.service is result for res in served)


def test_burst_admitted_callers_wake_on_failure():
    gate = threading.Event()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        raise KbStartupError("boom")

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=10.0, max_waiters=4)
    outcomes = []
    lock = threading.Lock()

    def worker():
        res = w.resolve()
        with lock:
            outcomes.append(res)

    threads = _run_threads(6, worker)
    try:
        _wait_for_admitted(w, 4)
        start = time.monotonic()
        gate.set()
        for t in threads:
            t.join(timeout=5)
        assert time.monotonic() - start < 2.0, "admitted waiters did not wake promptly"
    finally:
        gate.set()
        for t in threads:
            t.join(timeout=5)

    assert len(outcomes) == 6
    errors = {res.response["error"] for res in outcomes}
    # 2 overflow (warming_up) + 4 admitted woken by the failure (kb_unavailable).
    assert errors == {"warming_up", "kb_unavailable"}
    assert sum(1 for res in outcomes if res.response["error"] == "kb_unavailable") == 4


def test_waiter_slot_released_after_timeout_allows_new_admission():
    """Slots release in finally: after an admitted waiter times out, a later
    caller must be admitted rather than overflowed."""
    gate = threading.Event()

    def builder(_bootstrap):
        gate.wait(timeout=10)
        return object()

    w = KbWarmup(bootstrap=None, builder=builder, wait_seconds=0.05, max_waiters=1)
    try:
        assert w.resolve().response["error"] == "warming_up"  # timed out, slot freed
        with w._lock:
            assert w._waiters == 0
        # Admitted again (not overflow): admitted_total increments.
        assert w.resolve().response["error"] == "warming_up"
        with w._lock:
            assert w._admitted_total == 2
            assert w._overflow_total == 0
    finally:
        gate.set()
