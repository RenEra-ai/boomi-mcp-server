# Stage-1 QA round 3 — affected re-run for the L2-r6-01 crash fix (CLEAN)

Tree `6e2a7ffca29ab796fb34fae5f0b1995b80986d59`, baseline `3fd5027e…` unchanged. Clean run,
so no report file; one line appended to `agents/reports/INDEX.md`.

Scope: one scenario. The Stage-2 review found that the previous round's own fix made an
illegal all-call process-call root raise an unhandled `StopIteration` out of
`parse_process_ir_v1`. An exception escaping a parse entry point can look different through
the tool layer than in a unit test, so the public-boundary behaviour was measured rather
than inferred from the offline pin.

## Surface

Direct ProcessIR IS reachable publicly: `build_integration(action="plan", config={"authoring_request":
{...{"intent_kind": "process_ir", ... "units": [{"process_ir": ...}]}}})`. The request shape
was derived at run time from `get_schema_template(schema_name="AuthoringRequestV1")` and the
models it names — NOT from the implementation under test. `plan` runs the full intake parse
(where the exception was raised) and mutates nothing.

## Result — typed at the boundary, and zero mutation

All three illegal roots (`[pc, pc]`, `[pc, pc, pc]`, `[pc, pc, stop]`) return byte-identical
structured refusals; the legal singleton `[pc]` is ACCEPTED (`_success: true`,
`is_valid: true`), which is the discriminator that stops this passing via "everything is
refused". Component inventory 22 before / 22 after, `mutation_performed: false` on every
case.

Served envelope for the crash case, verbatim:

```json
{"_success": false,
 "authoring_diagnostics": [{
   "authoring_contract_entry_ids": ["diagnostic.process_ir_capability_process_call_return_path_binding_unsupported"],
   "code": "PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED",
   "message": "a process_call is the terminal of its path — a root sequence containing one admits no other step, including a trailing stop or return_documents (step 1)",
   "path": "/intent/units/0/process_ir/body/steps/1",
   "remediation": "Author the process call as the TERMINAL of its path and remove whatever followed it …"}],
 "error_code": "INVALID_INPUT",
 "mutation_performed": false}
```

## Non-vacuity — A/B against the PRE-FIX code, same public boundary

The identical four calls were run against a `git archive 6e2a7ff^ src` extraction. There the
two all-call roots return:

```json
{"_success": false, "error": "Integration builder failed: ", "exception_type": "StopIteration"}
```

— an untyped internal-error envelope with no code, no path, no remediation and no
contract-entry id. So the probe demonstrably detects the defect, and the fix specifically
closes it; this is not a probe that would have passed either way.

## Two harness facts recorded by QA (both traps, both now in its README)

1. The tool layer does NOT leak a traceback — it renders `exception_type`. So "assert no
   stack trace" is a VACUOUS assertion; the code is what must be asserted.
2. An extracted-tree A/B cannot be arranged on `sys.path` alone, because `server.py` does
   `sys.path.insert(0, <repo>/src)` at import. QA's first control run silently measured HEAD
   and looked like a pass until it printed `boomi_mcp.models.process_ir.__file__`. The
   control was only meaningful once the module file was asserted to be in the extracted tree.

## Observation, not a finding

The fix also moves the `[pc, pc, stop]` served pointer from `steps/2` (the trailing stop) to
`steps/1` (the second call) — the earliest offending step, now consistent across all three
illegal forms. A served-contract change in the intended direction, on a case that already
refused.
