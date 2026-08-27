# cap155-e5-delete-attested — PROVENANCE

## What this proves

A REST **DELETE** executed TWICE against the same live resource, with a counterparty access
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
| `execution-d999b19c-8f5f-4d46-9d93-11de5846c7a4-2026.08.27` | `2026-08-27T20:33:34.743823678Z` | `INFO:     192.168.65.1:55445 - "DELETE /admin/cdscm/api/v1/clients/df0c8de9-3a8b-4e13-b9a6-e08819540b24 HTTP/1.1" 204 No Content` | **204 No Content** |
| `execution-18b59781-67b7-4f31-afc6-89a619efd15b-2026.08.27` | `2026-08-27T20:33:47.419563295Z` | `INFO:     192.168.65.1:58648 - "DELETE /admin/cdscm/api/v1/clients/df0c8de9-3a8b-4e13-b9a6-e08819540b24 HTTP/1.1" 404 Not Found` | **404 Not Found** |

Attribution is unambiguous: `2` logged DELETE requests
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
each execution contributes a `GET` on the body-source path followed by the DELETE on the target
about 12 ms later, while each readback point contributes a GET triple.

## The verdict

Two executions, two logged DELETE requests, one per execution -- and their statuses DIFFER:
`204 No Content` then `404 Not Found`.

* Both executions reported success: ExecutionRecord `COMPLETE`, in=1 out=2, and BOTH
  GenericConnectorRecord rows read `SUCCESS` -- including the run whose request 404'd.
* The resource state converged: the target reads 404 at both R1 and R2, with identical
  bodies once the counterparty's own `timestamp` field is set aside.
* POSITIVE CONTROL, same run: the first DELETE moved the target from `200` to `404`, so the
  probe detects a change when there is one.
* NEGATIVE CONTROL, same run: an untouched control resource is byte-identical at R0, R1
  and R2.

So `rest.delete` on this counterparty is **effect-idempotent but not response-idempotent**:
replaying converges the state while returning a different status.

### What the platform record does and does not tell you

Measured (`analysis.json` -> `platform_discrimination`): the GCR `status` field is `SUCCESS`
for the 204 and the 404 alike, so status alone cannot classify the replay. But unlike the
PATCH case the platform is NOT blind here -- the send-stage row's `size` differs (0 vs 225)
and the served response document carries the counterparty's 404 body verbatim. A consumer
reading documents can distinguish them; a consumer reading only `status` cannot.

## Staged readbacks

| point | target HTTP | target business digest | control HTTP | control business digest |
|---|---|---|---|---|
| R0_before | 200 | `b3d9eb119701` | 200 | `f88abcae701f` |
| R1_between | 404 | `2759dbcaac9f` | 200 | `f88abcae701f` |
| R2_after | 404 | `2759dbcaac9f` | 200 | `f88abcae701f` |

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

This capture is the SECOND run of its scenario. In the first, the windows were padded by a
second at each bound and enclosed a settle sleep, so consecutive windows overlapped and the
replay's DELETE landed inside the overlap -- attributable to both executions, i.e. ambiguous.
An ambiguous attribution is a failed capture, so that attempt was VOIDED, the instrument was
tightened to bracket exactly the (synchronous) execute call, a clock-skew control was added
in place of the guessed padding, the target was re-seeded, and the scenario was re-run. The
recorded windows here are disjoint (`window_overlaps` is empty).

`clock_skew_control` in `analysis.json` records the measurement that justifies unpadded
bounds: a request issued between two host-clock marks is stamped by the container within
those marks, so the two clocks agree to within the round trip.

## Fixture provenance

Every component here was created by POSTing **banked, proven-operable component XML** via
`manage_component(action="create", config={"xml": …})`. The builder under test authored
nothing in this capture.

* `component_op_src.xml` <- `cap155-e2-delete/source_operation_component.xml` (banked sha256 `279806a7551e9907…`), verb `GET`, path bytes unedited: `True`
* `component_op_tgt.xml` <- `cap155-e2-delete/operation_component.xml` (banked sha256 `40633cb19b978923…`), verb `DELETE`, path bytes unedited: `True`
* `component_process.xml` <- `cap155-e3b-patch-canonical/component_process.xml` (banked sha256
  `e2bb0683eb13f412…`)

Only identity attributes (`componentId`, `version`, `createdDate`/`By`,
`modifiedDate`/`By`, `deleted`, `currentVersion`, `branchName`/`Id`) were stripped and the
root `name=` substituted; the process additionally had its `operationId` references remapped
to the re-created operations, and its send-stage `actionType` swapped to DELETE.
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
