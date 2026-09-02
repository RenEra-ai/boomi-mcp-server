# cap155-e6-post-attested — PROVENANCE

## What this proves

A REST **POST** executed TWICE against the same live resource, with a counterparty access
log that covers BOTH executions and attributes each logged request to a specific execution.

This is the same instrument, and the same two conjuncts, as
`cap155-e5-delete-attested` / `cap155-e5-patch-attested`: earlier rounds had one half each
-- `cap155-e2-post` ran two executions but archived no access log, and the `cap155-e4-*`
captures archived an access log for a single execution. The evidence registry requires a
retry verdict to rest on an OBSERVED replay -- as many logged requests of the verb under
test as there were executions -- so neither supported an affirmative verdict. Verified by
running it: `classify()` returns `UNKNOWN`/`UNVERIFIED` for this directory with
`mock_access_log.txt` removed, and again with the replay's logged request deleted.

Account `trainingglebbochkarov-16926N`,
runtime `renera-local-atom`, environment `Local Test Env`, 2026-09-01.

## The two executions and their requests

| execution id | log stamp | request | status |
|---|---|---|---|
| `execution-040dc855-b5c1-44fb-9382-b07a2d0f1705-2026.09.01` | `2026-09-01T19:54:06.583224299Z` | `INFO:     192.168.65.1:17527 - "POST /admin/cdscm/api/v1/matters HTTP/1.1" 201 Created` | **201 Created** |
| `execution-3752bda7-f0af-4518-8728-1f5cbf931c01-2026.09.01` | `2026-09-01T19:54:22.356875001Z` | `INFO:     192.168.65.1:22350 - "POST /admin/cdscm/api/v1/matters HTTP/1.1" 409 Conflict` | **409 Conflict** |

Attribution is unambiguous: `2` logged POST requests
on the attribution path, `2` executions, exactly one request per execution
(`True`), and `unattributed` is empty.

**How a request is attributed.** Each execution is bracketed by host-clock marks taken
around the synchronous `execute_process` call; `docker logs -t` stamps every line with
dockerd's clock; a line belongs to the execution whose window contains it, and a line
contained by two windows would be reported as unattributed rather than guessed. The
recorded windows are disjoint (`window_overlaps` is empty). Attribution is computed on
PARSED datetimes -- an RFC3339 `…Z` stamp compared against a `+00:00` bound as STRINGS
orders correctly only while the seconds differ -- and the string-comparison pass is run
beside it as a control: `agrees_with_string_comparison_pass` =
`True`.

Peer IP is NEVER used to attribute: the atom and this QA host both appear as
`192.168.65.1` in the log, so the address cannot separate the runtime's requests from the
readbacks. Attribution rests on (verb, path, time-window).

**The attribution path and the readback path are DIFFERENT here, and that is the
verb's shape, not a shortcut.** The banked POST operation names the COLLECTION
`/admin/cdscm/api/v1/matters`; the resource whose state moves is the item the POST creates.
The counterparty's `create_document` honours a caller-supplied `key`, and the body the send
stage transmits is the fetch stage's output document, so the created item lands at
`/admin/cdscm/api/v1/matters/c4016c66-fb7c-4fab-bb2e-edda725d6b89` -- the template's key, in the
`matters` collection. Attribution keys on the logged request's path (the collection);
convergence keys on the item. The item was verified ABSENT before the run (`404`), so the
first call's effect is a presence change rather than a field diff.

The full window is archived, not just the matching lines, so the readbacks are visible too.

`clock_skew_control` in `analysis.json` records the measurement that justifies unpadded
window bounds: a request issued between two host-clock marks is stamped by the container
within those marks (`stamp_within_host_bracket` =
`True`, round trip
12 ms), so the two clocks agree to
within the round trip.

## The verdict

Two executions, two logged POST requests, one per execution -- and their statuses DIFFER:
`201 Created` then `409 Conflict`.

* Both executions reported success: ExecutionRecord `COMPLETE`, in=1 out=2, and BOTH
  GenericConnectorRecord rows read `SUCCESS` -- including the run whose request was
  REFUSED with a 409.
* The resource state converged: the target reads `200` at both R1 and R2 with a
  byte-identical body -- the conflicting POST wrote nothing at all, so not even
  `modifiedOn` moved.
* POSITIVE CONTROL, same run: the first POST moved the target from `404` to `200`, so the
  probe detects a change when there is one.
* NEGATIVE CONTROL, same run: an untouched control resource is byte-identical at R0, R1
  and R2.

So `rest.post` on this counterparty is **create-only**: the first call creates, the replay
is refused, and the refusal leaves the created resource untouched. That is the
conflict-without-second-effect outcome -- the same shape as the archived DELETE, reached
from the other direction (DELETE converges on absence, POST on presence).

### What the platform record does and does not tell you

Measured (`analysis.json` -> `platform_discrimination`): the GenericConnectorRecord
`status` field is `SUCCESS` for the 201 and the 409 alike, so status alone cannot classify
the replay. As with DELETE the platform is not wholly blind -- the send-stage row's `size`
differs (301 vs 186) and the served response document carries the counterparty's 409 body
verbatim -- but a consumer reading only `status` sees two identical successes. The channel
that carries the refusal is the counterparty log and the readback, which is the conjunct
this capture supplies.

## Staged readbacks

| point | target HTTP | target business digest | control HTTP | control business digest |
|---|---|---|---|---|
| R0_before | 404 | `bb09de49c4f0` | 200 | `29ca0146052c` |
| R1_between | 200 | `95837de84b6a` | 200 | `29ca0146052c` |
| R2_after | 200 | `95837de84b6a` | 200 | `29ca0146052c` |

The **control** is a resource referenced by no operation component in this capture; it is
byte-identical across all three points (`CONTROL_untouched_R0_R1_R2` =
`True`). Convergence of the target means nothing without it.
Its non-vacuity was measured, not assumed: replacing the R2 target readback with the R0 one
-- a replay that moved the state -- turns this capture's verdict into `non_idempotent`.

The convergence key is the readback STATUS plus a sha256 over the body with
`modifiedOn`, `modifiedBy` and `timestamp` stripped. `timestamp` is in that set because the
counterparty's error envelopes carry their own clock, so comparing raw bodies would report
two identical absences as "not converged" -- a defect in the measurement, not in the system.

* `first_write_changed_business_state`: `True`
* `resource_state_converged_R1_R2`: `True`
* `full_state_identical_R1_R2`: `True`
* `body_source_stable_across_runs`: `True`

## Fixture provenance

Every component here was created by POSTing **banked, proven-operable component XML** via
`manage_component(action="create", config={"xml": …})`. The builder under test authored
nothing in this capture.

* `component_op_src.xml` <- `cap155-e2-post/source_operation_component.xml` (banked sha256 `279806a7551e9907…`), verb `GET`, path bytes unedited: `True`
* `component_op_tgt.xml` <- `cap155-e2-post/operation_component.xml` (banked sha256 `41a0edd40dcc1f05…`), verb `POST`, path bytes unedited: `True`
* `component_process.xml` <- `cap155-e3b-patch-canonical/component_process.xml` (banked sha256
  `e2bb0683eb13f412…`)

Only identity attributes (`componentId`, `version`, `createdDate`/`By`,
`modifiedDate`/`By`, `deleted`, `currentVersion`, `branchName`/`Id`) and the FOLDER
attributes (`folderFullPath`, `folderId`, `folderName`) were stripped, and the root `name=`
substituted. The folder strip is new relative to the E5 captures and is forced by the
account roll: the banked bytes carry the folder id of `traininghlibbochkarov-JKIY2X`, which
this account has no row for.

The process additionally had its references remapped and, where the verb differs from the
banked process's send stage, that stage's `actionType` swapped:

* operation/connection remap: `{"03ff92d3-46db-4b45-9a52-ccf6de6613ca": "f9be9156-2ea2-43f0-acac-b835a7097db7", "9f2e2ad7-518b-4485-9ab9-e0bbbde75ae2": "90b5c5e4-3496-460d-b5a7-5f1557dfeba4", "c4281346-83e9-4026-856d-ede718ec68a0": "2fe488e4-3169-4529-9515-d854570c8ffc"}`
* send `actionType`: `{"from": "PATCH", "to": "POST"}`

The connection remap is likewise forced by the roll -- the banked process names the dead
account's REST connection. The live connection was read back before use and points at
`http://host.docker.internal:8081`, the same counterparty. Full digests and the exact
substitutions are in `fixture_derivation.json`, and the persisted values are re-read from
the platform (`operation_ids_in_readback`, `connection_ids_in_readback`,
`send_action_types_in_readback`) rather than assumed from the bytes that were sent.

**The path bytes were not edited.** The counterparty accepts a caller-supplied `key`, so
the resources were re-seeded at the exact keys the banked operation XML already points at.

Other provenance:
* counterparty semantics -> `../sandbox-services` (`rest/app/routers/entities.py`, which
  states the three write verbs' replay behaviour outright), re-measured at run time,
  causally independent of this repo.
* live ids -> read back from the account at run time.
* every asserted expectation comes from the LIVE platform response or the LIVE counterparty
  readback -- never from the implementation under test.

## A note on harvest timing

`ExecutionConnector` is indexed LATER than `ExecutionRecord`. Measured on this account: a
query issued immediately after a `COMPLETE` execution returned ZERO rows, and archiving that
would have produced a capture whose second run carries no platform connector row at all --
which is exactly what placement and the document counts are read from. The harvester polls
that endpoint and asserts a non-empty result before archiving; both runs here carry four
rows apiece.

## Files

`records.json` indexes all 37 archived files with digest, byte length and what each
proves. The archive-wide checksum manifest is
`docs/architecture/evidence/issue-155/SHA256SUMS`, which the ingest verifies every capture
against before interpreting a byte of it.

Connector documents are named by a stable ORDINAL (`run1_doc00…`) derived from a
deterministic sort of the GenericConnectorRecord id, never by a guessed role -- measured
again here: the platform returns the same four rows in a DIFFERENT order on the two
executions, so a role-named file would collide or mislabel.

`run*_execution_connector_raw.json` and `run*_generic_connector_record_raw.json` preserve the
RAW platform envelopes. The flattened rows drop `executionId` and the ingest correlates per
record on that field, so the raw envelope is what makes the rows correlatable; the flattened
copies in `run*_connector_rows.json` / `run*_gcr_rows.json` RETAIN `executionId` as well.

## Redaction and index integrity

Boomi stamps `createdBy`/`modifiedBy="<user email>"` into every component XML readback;
those are redacted to `redacted@example.invalid` here. **Redaction runs BEFORE indexing**,
so every digest and byte length describes the bytes actually on disk. The writer re-reads
every indexed digest from disk after writing and additionally sweeps every archived file for
a residual email; this directory was reported CLEAN on both. Every file is indexed (no
orphans), no filename appears twice, and no indexed file is zero-byte.
