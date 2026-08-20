# Stage-1 QA round 2 — the post-correction re-run (CLEAN, zero findings)

Tree `f8b56ef5701004bec06e430dcb688346a3cc63cb`, baseline `3fd5027e…` unchanged. Run
clean, so the agent produced no report file; one line was appended to
`agents/reports/INDEX.md`. This file records the load-bearing measurements so the audit
record does not depend on a gitignored artifact.

This round exists because CLAUDE.md makes an applied Stage-1 correction's affected live
QA re-run UNCONDITIONAL, and the §6 architect review correctly flagged that it was owed:
the three round-1 findings were fixed and suite-validated, but the most serious of them
could only be re-confirmed live.

## QA-175-r1-01 — closed, measured the way it was found

Plan, served verbatim:

```
steps: [('first', 'create', None, None),
        ('second', 'error_process_validation', 'PROCESS_CALL_CONFIG_INVALID', 'reliability.catch_exception')]
```

Apply: `_success: false`, `"Plan contains unresolvable steps. No operations were executed."`,
`partial_results: {}`.

**The measurement that failed in round 1**: component inventory **BEFORE 22 → AFTER 22,
`NEW: {}`**. Round 1 measured **26 → 27** — a component really created before the refusal.

**Non-vacuity**: the same probe function observed 22→23 and 24→25 on genuine creates later
in the same run, so "no change" is a detection rather than a blind instrument.

**Precision controls — the new plan-time gate does not over-fire.** All four of these still
plan `create`: `error_subprocess_ref` alone; `document_cache_ref` + `catch_exception`;
`error_subprocess_ref` + `catch_notify`; a bare `catch_exception`. A single-component bad
spec also refuses at plan, so the gate is not order-dependent.

## QA-175-r1-03 — closed on the served surface

`get_schema_template(resource_type="process", operation="create",
protocol="database_to_api_sync")` now publishes `PROCESS_CALL_CONFIG_INVALID` on field
`reliability.catch_exception`. The #108 note states the error-subprocess exclusion and that
the refusal happens "at PLAN time, before anything is created" — a served claim item 1
measures TRUE rather than merely asserting. The #89 note records the terminal
error-subprocess route. All three round-1 stale strings scan to `[]`, with a positive
control proving the scanner finds the new code in the same document.

## QA-175-r1-02 — closed, both arms exercised

```
step 'first':  status='created' component_id='5d7dfb6c-…' _success=True
step 'second': status='failed'  component_id=None        _success=False
```

and the arm round 1 never reached — an update aimed at a nonexistent id:

```
"status": "refused", "component_id": null,
"result": {"write_attempted": false, "error_code": "UPDATE_PRESERVATION_FETCH_FAILED"}
```

with inventory 25→25, and a control `update` on the real component returning
`status: "updated"`.

## No regression

Single-call wrapper: shapes `['start','processcall']`, zero `stop` shapes, empty
`returnpaths` and `dragpoints`, `verify` → `{"verified": true, "errors": [], "warnings": [],
"shapes_checked": 2}`. Stored bytes verbatim:
`<processcall abort="false" processId="31bb57cd-…" wait="true"><parameters/><returnpaths/></processcall></configuration><dragpoints/>`.

## Not measured, stated plainly

The Chrome plugin is still not connected (`list_connected_browsers` → `[]`). Probe 2 (a
UI-built returning child and parent) and the post-fix UI canvas check are unmeasured for a
second round and carry to #176 unchanged. Neither affects this slice's severity: round 1's
in-tree UI-built captures already answered the pairing-contract question, and the runtime
evidence is stronger than a canvas observation.

## Non-blocking observations recorded by QA (no action taken, none asked)

- The legacy `_apply_plan` path emits no `mutation_status` key, so `status` is its only
  mutation accounting. Unchanged from baseline, outside this fix.
- `_components_were_written()` is imported only by tests — pre-existing, not served.
- A failed PLATFORM create carries no `write_attempted` key at all, so it lands on the
  `failed` fallback. Correct today; a future change that started stamping it would silently
  move that arm to `refused`, which the new standing harness now pins.

## Account hygiene

3 components created, 3 deleted, 0 deployed. Process count 22 → 25 → 22 (the exact pre-run
count). Deletes ran from an explicit allowlist with a name assertion, never harvested from
apply results. Working tree byte-identical before and after.
