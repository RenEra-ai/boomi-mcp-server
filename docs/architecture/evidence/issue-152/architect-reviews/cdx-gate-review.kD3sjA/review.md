The three round-13 reproductions are closed, but I found three remaining fail-open paths. The code is not ready to land.

- [P1] `scripts/wave_gate.py:1517` — Execution is reconciled only by aggregate count, not node identity. Reproduced with collected nodes A and B, where B always fails, plus a `pytest_collection_modifyitems` hook that replaces B with A only during the full run. Collection reports A+B; execution runs A twice and reports `2 passed`; `run_suite()` accepts because `passed + skipped == len(collected)`. Reconcile per-node execution outcomes and reject missing or duplicate executed node IDs.

- [P2] `scripts/wave_gate.py:944` — GitHub event JSON accepts duplicate authority fields with last-value-wins semantics. I supplied duplicate, conflicting `before` and `after` members; the manifest phase selected the later pair and exited 0 instead of rejecting the ambiguous event. Parse event payloads with duplicate-key rejection and emit `BASELINE_EVENT_INVALID`.

- [P2] `scripts/wave_gate.py:1736` — Render-envelope JSON has the same duplicate-member hole. Conflicting duplicate `id`, `len`, `sha256`, and `b64` members collapse before the exact-key and integrity checks. Emitting the malformed envelope in both passes against matching bytes made `check_goldens()` pass. Reject duplicate members during parsing with `GOLDEN_RENDER_FAILED`.

VERDICT: ISSUES FOUND
