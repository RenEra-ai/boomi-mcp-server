# cap155-e3-patch-legacy — PROVENANCE

## What this proves
A REST PATCH executed TWICE against the same resource with an identical payload, through
the PUBLIC MCP tool boundary, on the legacy `integration_spec` / `sync_pipeline` route
(fetch GET -> send PATCH) — the route E1/E2 proved carries a non-GET REST verb.

Account `traininghlibbochkarov-JKIY2X`, runtime `renera-local-atom`
(89387bdf-b177-4631-9cc6-687bf4afefa7), environment `Local Test Env`
(c8d9140d-6d34-4ca6-91ff-bfd263556d5b), 2026-08-27.

## The verdict (see analysis.json)
* Both executions reported success: ExecutionRecord `COMPLETE`, in=1 out=2, connector
  errorCount 0, PATCH GenericConnectorRecord `SUCCESS`.
* The payload really was identical: the PATCH request body is the GET stage's output
  document, byte-identical across the two runs, with the source resource unmoved at all
  three read points.
* The second identical PATCH left the resource's BUSINESS state unchanged but did NOT
  leave the full state identical: exactly ONE field differs, `modifiedOn`.
* `createdOn` is stable across R0..R2, so the replay merged into the existing row rather
  than replacing or recreating it.
* Positive control in the SAME run: the FIRST PATCH did change the business digest
  (R0 -> R1), so the probe demonstrably detects a change when there is one.

## Files
`run1_*` / `replay_*` — execution record, execution-connector rows, generic connector
records, and one file per (run, shape) connector document. Document filenames carry a
stable ORDINAL (`run1_doc00_…`, `run1_doc01_…`), so no two shapes can ever collide on a
filename. A shape for which the platform served no document bytes is recorded in
`records.json` as an explicit ABSENCE (`file: null`, `document_present: false`, with the
`intended_file` name) rather than as an empty file.
`readback_R0_before|R1_between|R2_after_{target,template}.json` — the three read points,
for both the PATCHed resource and the body source.
`component_op_*.xml` — stored Boomi component XML for each REST operation.
`account_liveness.json` — C1, redacted. `analysis.json` — the derived verdict.

## Fixture provenance
* Live component/operation/connection ids: read back from the account at run time.
* Counterparty semantics and the `modifiedOn` control: `../sandbox-services` @`e25849a`
  (see cap155-e3-patch-counterparty), causally independent of this repo.
* Spec/request shapes: the E1/E2 capture harness frozen before this dispatch.
* Every asserted expectation comes from the LIVE platform response or the LIVE counterparty
  readback — never from the implementation under test.

## Limits
The connector document store serves ONE document per GenericConnectorRecord — the OUTPUT
document. The PATCH request bytes were therefore not captured directly; identity of the
payload is established TRANSITIVELY, via the GET stage's byte-identical output document,
which is what the send stage transmits.

## Redaction and index integrity
Boomi stamps `createdBy`/`modifiedBy="<user email>"` into every component XML readback; in
this capture both attribute values are `[redacted]`. Only those two attributes change — the
connector configuration this capture exists to evidence is untouched. `account_liveness.json`
keeps only liveness-bearing fields.
**Redaction runs BEFORE indexing**, so every digest and byte length in `records.json` and
`SHA256SUMS` describes the bytes actually on disk. Every file in this directory is indexed
(no orphans), no filename appears twice, and no indexed file is zero-byte.
