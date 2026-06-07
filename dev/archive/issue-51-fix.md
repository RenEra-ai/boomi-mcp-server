# Fix plan — code-review findings on commit 313ce04 (#51 Try/Catch + DLQ)

## Context

Commit `313ce04` ("feat: emit verified Try/Catch + DLQ catch-path for retry_count==0 (#51)") was vetted with a recall-mode multi-angle code review (`/cr 313ce04`). It surfaced 15 findings that collapse to 6 distinct issues. After verifying each against the code (read `_check_process_flow_ref_types`, the classifiers, `_emit_try_catch_shapes`, the tests) and validating the fix design with a Plan agent, this plan addresses the genuine ones. The earlier §6 Codex review missed the top issue because it did not trace into `integration_builder.py`.

**Why this matters:** issue #51 un-gated two new `$ref`-bearing config slots (`reliability.dlq.document_cache_id` / `process_id`) without extending the issue #49 plan-time type-discipline to them (Fix A), and the new build-path emitter is not "total" on the validate-bypass path the way the rest of `build()` is (Fix B). Both are real defects; the rest are cheap hardening/cleanup.

Scope is M3.R1a only. All edits stay in the existing files; no new modules, no new dependencies.

## Fixes

### Fix A — extend the issue #49 `$ref` type-check to the DLQ slots (the main fix)
**File:** `src/boomi_mcp/categories/integration_builder.py`, `_check_process_flow_ref_types` (~1062-1132).

- Extract, near the existing `source`/`target` extraction (~1078-1079):
  - `reliability = raw_config.get("reliability") if isinstance(..., dict) else {}`
  - `dlq = reliability.get("dlq") if isinstance(..., dict) else {}`
  - `dlq_mode = str(dlq.get("mode") or "").strip().lower()`
- **Mode-gate the new slot rules** (the Plan-agent adjustment — `_validate_dlq_binding` only validates the active mode's fields, so an unconditional rule could surprise-reject a stray cross-mode `$ref` the build never reads). Append to `slot_rules` only the rule matching the active mode:
  - `document_cache_ref` → `("reliability.dlq.document_cache_id", dlq.get("document_cache_id"), "Document Cache")`
  - `error_subprocess_ref` → `("reliability.dlq.process_id", dlq.get("process_id"), "error subprocess")`
- Add two explicit `elif` arms in the classify loop **before** the final `else: ok = True` (~1112-1113):
  - `elif expected_role == "Document Cache": ok = _effective_component_type(target_comp) == "documentcache"`
  - `elif expected_role == "error subprocess": ok = _effective_component_type(target_comp) == "process"`
- No change needed to `_format_actual_role` — it falls through to `return effective` (~659), giving readable `documentcache`/`process` in the mismatch message.

The loop already skips non-`$ref`/literal values and outside-spec refs (`continue` at ~1091-1098), so literal-id and disabled-DLQ configs are untouched. Reuses `_effective_component_type` (~539) — no new classifier.

**Tests** (`tests/test_integration_builder.py`):
- Add stub roles to `_STUB_DEP_ROLES` (~2654) and `_stub_dep_comp` (~2662): a `documentcache` stub and a `process` stub, each `action="update"` + synthetic `component_id`, `type="documentcache"`/`type="process"`, minimal config. **The process stub must NOT carry `process_kind`** (or it trips the process-flow validation block at ~2437-2442); `documentcache` has no create path (`reference_only`-style only).
- Mirror the swapped-ref tests in `TestBuildPlanProcessFlowRefTypes` (~3561-3777): a `$ref:document_cache_id` pointing at a non-cache and `$ref:process_id` at a non-process → `_build_plan` step `validation_error.error_code == "PROCESS_REF_TYPE_MISMATCH"` with the right `field`; plus a positive case where the `$ref` points at the correct `reference_only` stub → plans `create` cleanly.

### Fix B — make `_emit_try_catch_shapes` total on the build()-bypass path
**File:** `src/boomi_mcp/categories/components/builders/process_flow_builder.py`, `_emit_try_catch_shapes` (~970-1016).

In the mode `if/elif` (~1009-1015), after computing `cache_id`/`process_id` **inside each arm**, raise when empty:
- empty binding → `raise BuilderValidationError(error_code="PROCESS_DLQ_BINDING_INVALID", field="reliability.dlq.document_cache_id"|"...process_id", ...)` (same code `_validate_dlq_binding` uses).
- add a final `else:` arm → `raise BuilderValidationError(error_code="PROCESS_XML_VALIDATION_FAILED", field="reliability.dlq.mode", ...)` for an unexpected mode (defensive; `_should_emit_try_catch` guards it; also closes the latent "future mode in `_TRY_CATCH_DLQ_MODES` with no branch → dangling Catch `toShape`" hazard).

This mirrors build()'s existing totality pattern (empty-name raise ~380-386). `_execute_component` already catches `BuilderValidationError` around build (~1537-1544), so this surfaces as a structured error, not a crash. No existing test calls build() with a DLQ mode but no binding, so no regression.

**Test:** add a direct-`build()` test asserting the empty-binding raise (mode set, id absent) → `PROCESS_DLQ_BINDING_INVALID`.

### Fix C — close the `$ref` DLQ resolution→emission coverage gap
**File:** `tests/test_process_flow_builder_trycatch_dlq.py`.

Add a test importing `_resolve_dependency_tokens` from `integration_builder`: build a cfg with `reliability.dlq.document_cache_id="$ref:my_cache"`, run `_resolve_dependency_tokens(cfg, {"my_cache": "<uuid>"})`, then `ProcessFlowBuilder.build(...)`, and assert the emitted `docCache == "<uuid>"`. This exercises the full `$ref`→resolve→emit invariant that today is only tested up to `validate_config`.

### Fix D — mode-neutral `catcherrors` userlabel
**File:** `process_flow_builder.py`, `_emit_catcherrors` (~1038) + regenerate the golden fixture.

Change the hardcoded `userlabel="Try/Catch all errors (no retry) - route caught documents to DLQ"` to a mode-neutral phrasing (e.g. "... route caught documents to the failure handler"), since for `error_subprocess_ref` the catch leg is a `processcall`, not a DLQ cache. The catch-leg shapes' own labels are already mode-accurate and unchanged. Regenerate `tests/fixtures/golden_xml/try_catch_dlq_document_cache.xml` (the userlabel is the only place this string appears besides source; the golden test compares canonicalized XML; no test asserts the exact userlabel).

### Fix E — trivial cleanups
**File:** `process_flow_builder.py`.
- build() branch (~462): `reliability_cfg.get("dlq") or {}` → `reliability_cfg["dlq"]` (the `_should_emit_try_catch` guard proves `dlq` is a non-empty dict).
- `_emit_try_catch_shapes` (~987-989): derive `catcherrors_name = f"shape{catcherrors_index}"` and `first_try_name = f"shape{first_try_index}"` instead of hardcoded `"shape2"`/`"shape3"` (consistency with `catch_name`, removes drift risk).

### Rejected — F (no change)
Leave `dlq.mode` normalized at the 3 sites (`_should_emit_try_catch` default `""`, `_validate_reliability` default `"disabled"` (load-bearing), `_emit_try_catch_shapes` default `""`). They run in different phases with different defaults; consolidating would couple the emitter to the guard for negligible gain.

## Critical files
- `src/boomi_mcp/categories/integration_builder.py` — `_check_process_flow_ref_types` slot rules + classify arms (Fix A).
- `src/boomi_mcp/categories/components/builders/process_flow_builder.py` — `_emit_try_catch_shapes` raises (B), `_emit_catcherrors` userlabel (D), build() branch + name derivation (E).
- `tests/test_integration_builder.py` — `_STUB_DEP_ROLES` / `_stub_dep_comp` + new `TestBuildPlanProcessFlowRefTypes` DLQ cases (A).
- `tests/test_process_flow_builder_trycatch_dlq.py` — empty-binding raise test (B), `$ref` resolution→emit test (C).
- `tests/fixtures/golden_xml/try_catch_dlq_document_cache.xml` — regenerate (D).

## Verification (per the repo completion workflow)
1. **Unit:** `PYTHONPATH=src .venv/bin/python -m pytest tests/ -q` → all pass (currently 3147; +new A/B/C tests). Confirm no regression in `TestBuildPlanProcessFlowRefTypes` / `test_process_flow_builder_trycatch_dlq.py` / `test_schema_template_process_flow.py`.
2. **Live QA:** dispatch `boomi-qa-tester` on the fix — verify via `.fn()` that (a) a `$ref` DLQ binding pointing at a wrong-type in-spec component now returns `PROCESS_REF_TYPE_MISMATCH` at plan; (b) a correct `reference_only`-bound `$ref` plans cleanly; (c) a direct `build()` with a DLQ mode but no binding raises `PROCESS_DLQ_BINDING_INVALID`; (d) the literal-id and disabled-DLQ paths are unchanged.
3. **Codex re-review of the FIX DELTA only:** run the repo's Codex gate scoped to the fix. The fix delta is small, but `codex-companion` has been wedging on large `git diff` output this session — if it hangs again, fall back to the `codex-impl-reviewer` subagent on the healthy `codex-drive` runtime (instructed to read changed files in ranges, avoid the live Boomi MCP). Iterate until clean.
4. Then resume the #51 finish (push `codex/issue-51` → `dev`, close #51, archive plan) per the earlier decision.
