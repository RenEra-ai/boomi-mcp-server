# cap155-e5-patch-attested — PROVENANCE

## What this proves

A REST **PATCH** executed TWICE against the same live resource, with a counterparty access
log that covers BOTH executions and attributes each logged request to a specific execution.

Earlier captures in this issue had one half each: `cap155-e2-*` / `cap155-e3*` ran two
executions but archived no access log, and `cap155-e4-*` archived an access log for a single
execution. The evidence registry requires a retry verdict to be supported by an OBSERVED
replay -- as many logged requests of the verb under test as there were executions -- so
neither supported an affirmative verdict. This capture supplies both conjuncts.

Account `traininghlibbochkarov-JKIY2X`,
runtime `renera-local-atom`, environment `Local Test Env`, 2026-08-27.

## The two executions and their requests

| execution id | log stamp | request | status |
|---|---|---|---|
| `execution-58bc6079-7b94-4793-b110-113d5f951dd3-2026.08.27` | `2026-08-27T20:22:58.246626134Z` | `INFO:     192.168.65.1:18270 - "PATCH /admin/cdscm/api/v1/clients/41172938-b9bd-4495-b411-9851f5ac7b00 HTTP/1.1" 200 OK` | **200 OK** |
| `execution-7d1f0df7-282c-4dfa-886e-91c5ab506f24-2026.08.27` | `2026-08-27T20:23:17.877421879Z` | `INFO:     192.168.65.1:39148 - "PATCH /admin/cdscm/api/v1/clients/41172938-b9bd-4495-b411-9851f5ac7b00 HTTP/1.1" 200 OK` | **200 OK** |

Attribution is unambiguous: `2` logged PATCH requests
on the target path, `2` executions, exactly one request per execution, and
`unattributed` is empty.

**How a request is attributed.** Each execution is bracketed by host-clock marks taken
around the synchronous `execute_process` call; `docker logs -t` stamps every line with
dockerd's clock; a line belongs to the execution whose window contains it, and a line
contained by two windows is reported as unattributed rather than guessed. Attribution is
computed on PARSED datetimes -- an earlier pass compared an RFC3339 `…Z` stamp against a
`+00:00` bound as strings, which happens to order correctly only while the seconds differ;
both results are recorded and `agrees_with_string_comparison_pass` reports whether they
matched.

Peer IP is NEVER used to attribute: the atom and this QA host both appear as
`192.168.65.1` in the log, so the address cannot separate the runtime's requests from the
readbacks. Attribution rests on (verb, path, time-window); the target path is a per-capture
GUID no other traffic touches.

The full window is archived, not just the matching lines, so the readbacks are visible too:
each execution contributes a `GET` on the body-source path followed by the PATCH on the target
about 12 ms later, while each readback point contributes a GET triple.

## The verdict

Two executions, two logged PATCH requests, one per execution, both `200 OK`.

* Both executions reported success: ExecutionRecord `COMPLETE`, in=1 out=2.
* The payload really was identical: the PATCH body is the GET stage's output document,
  byte-identical across the two runs, with the body source unmoved at all three read points.
* The second identical PATCH left the resource's BUSINESS state unchanged (R1 and R2 share
  a business digest) but did NOT leave the full state identical: exactly ONE field differs,
  `modifiedOn`.
* `createdOn` is stable R0..R2, so the replay merged into the existing row rather than
  replacing or recreating it.
* POSITIVE CONTROL, same run: the FIRST PATCH did change the business digest (R0 -> R1), so
  the probe demonstrably detects a change when there is one.
* NEGATIVE CONTROL, same run: an untouched control resource, referenced by no operation
  component, is byte-identical at R0, R1 and R2. Its stability is what makes the target's
  convergence mean something.

So `rest.patch` on this counterparty is **conditionally idempotent**: the business state
converges, the full served state does not.

### What no platform record can tell you

Measured, not assumed (`analysis.json` -> `platform_discrimination`): across the two runs
**no** served platform field discriminates the replay from the original. The
GenericConnectorRecord rows are `SUCCESS`/`SUCCESS`, the same `size`, and the served
response documents are identical; the access log shows `200 OK` for both. The ONLY channel
that reveals the replay is the counterparty READBACK, via `modifiedOn`. A retry verdict for
this verb therefore cannot be derived from execution records alone -- which is exactly the
conjunct this capture exists to supply.

## Staged readbacks

| point | target HTTP | target business digest | control HTTP | control business digest |
|---|---|---|---|---|
| R0_before | 200 | `529fb5fe84e1` | 200 | `8c915b125340` |
| R1_between | 200 | `90066496cfbf` | 200 | `8c915b125340` |
| R2_after | 200 | `90066496cfbf` | 200 | `8c915b125340` |

The **control** is a resource referenced by no operation component in this capture; it is
byte-identical across all three points (`CONTROL_untouched_R0_R1_R2` =
`True`). Convergence of the target means nothing without it.

The convergence key is the readback STATUS plus a sha256 over the body with
`modifiedOn`, `modifiedBy` and `timestamp` stripped. `timestamp` is in that set because the
counterparty's 404 envelope carries its own clock: comparing raw bodies reported two
identical absences as "not converged", a defect in the measurement, not in the system. The
key was re-derived to fit BOTH readback shapes and re-checked against the 200 shape, whose
bodies carry no `timestamp` at all -- so widening the set cannot silently move the 200-case
result.

## A note on the attribution windows

The two host-clock windows in this capture OVERLAP by about two seconds, an artefact of the
first instrument revision, which padded each bound by a second and enclosed a settle sleep.
No PATCH request falls inside the overlap, so every request is still attributable to exactly
one execution and `unattributed` is empty -- but the overlap is recorded in
`access_log_attribution.json` rather than hidden, because it is the property that would
have made attribution ambiguous had a request landed there. The DELETE capture was re-run
with unpadded windows and has no overlap at all.

## Fixture provenance

Every component here was created by POSTing **banked, proven-operable component XML** via
`manage_component(action="create", config={"xml": …})`. The builder under test authored
nothing in this capture.

* `component_op_src.xml` <- `cap155-e3b-patch-canonical/component_op_src.xml` (banked sha256 `22a101efd3d691d2…`), verb `GET`, path bytes unedited: `True`
* `component_op_tgt.xml` <- `cap155-e3b-patch-canonical/component_op_patch.xml` (banked sha256 `afc8ba001fa65487…`), verb `PATCH`, path bytes unedited: `True`
* `component_process.xml` <- `cap155-e3b-patch-canonical/component_process.xml` (banked sha256
  `e2bb0683eb13f412…`)

Only identity attributes (`componentId`, `version`, `createdDate`/`By`,
`modifiedDate`/`By`, `deleted`, `currentVersion`, `branchName`/`Id`) were stripped and the
root `name=` substituted; the process additionally had its `operationId` references remapped
to the re-created operations.
Full digests and the exact substitutions are in `fixture_derivation.json`.

**The path bytes were not edited.** The counterparty accepts a caller-supplied `key`, so the
resources were re-seeded at the exact keys the banked operation XML already points at. This
is stronger than the `cap155-e4-*` captures, which substituted the path.

Other provenance:
* counterparty semantics -> `../sandbox-services`, re-measured at run time, causally
  independent of this repo.
* live ids -> read back from the account at run time.
* every asserted expectation comes from the LIVE platform response or the LIVE counterparty
  readback -- never from the implementation under test.

## Files

`records.json` indexes all 37 files with digest, byte length and what each proves;
`SHA256SUMS` carries the digests alone. Connector documents are named by a stable ORDINAL
(`run1_doc00…`) derived from a deterministic sort of the GenericConnectorRecord id, never by
a guessed role -- measured: the platform returns the same four rows in a DIFFERENT order on
the two executions, so a role-named file would collide or mislabel.

`run*_execution_connector_raw.json` and `run*_generic_connector_record_raw.json` preserve the
RAW platform envelopes. The flattened rows drop `executionId` and the ingest correlates per
record on that field, so the raw envelope is what makes the rows correlatable; the flattened
copies in `run*_connector_rows.json` / `run*_gcr_rows.json` RETAIN `executionId` as well.

## Redaction and index integrity

Boomi stamps `createdBy`/`modifiedBy="<user email>"` into every component XML readback; those
are redacted to `redacted@example.invalid` here. **Redaction runs BEFORE indexing**, so every
digest and byte length describes the bytes actually on disk. The writer re-reads every
indexed digest from disk after writing and additionally sweeps every archived file for a
residual email; this directory was reported CLEAN on both. Every file is indexed (no
orphans), no filename appears twice, and no indexed file is zero-byte.
