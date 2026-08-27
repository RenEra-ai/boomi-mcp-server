# cap155-e3-patch-legacy — PROVENANCE

## What this proves
A REST PATCH executed TWICE against the same resource with an identical payload,
through the PUBLIC MCP tool boundary, on the legacy `integration_spec` /
`sync_pipeline` route (fetch GET -> send PATCH) — the route E1/E2 proved carries a
non-GET REST verb.

Account `traininghlibbochkarov-JKIY2X`, runtime `renera-local-atom`
(89387bdf-b177-4631-9cc6-687bf4afefa7), environment `Local Test Env`
(c8d9140d-6d34-4ca6-91ff-bfd263556d5b), 2026-08-27.

## The verdict (see analysis.json)
* Both executions reported success: ExecutionRecord `COMPLETE`, in=1 out=2,
  connector errorCount 0, PATCH GenericConnectorRecord `SUCCESS`.
* The payload really was identical: the PATCH request body is the GET stage's output
  document, and that document is byte-identical across the two runs
  (`f2d267f9…`), with the source resource unmoved at all three read points.
* The second identical PATCH left the resource's BUSINESS state unchanged
  (`2d2807f3…` at both R1 and R2) but did NOT leave the full state identical:
  exactly ONE field differs, `modifiedOn`
  (run1 `2026-08-27T06:01:33.254Z` -> replay `2026-08-27T06:01:47.290Z`).
* `createdOn` is stable across R0..R2, so the replay merged into the existing row
  rather than replacing or recreating it.
* Positive control in the SAME run: the FIRST PATCH did change the business digest
  (R0 -> R1), so the probe demonstrably can detect a change when there is one.

## Files
`run1_*` / `replay_*` — execution record, execution-connector rows, generic connector
records, and the downloaded connector documents for each execution.
`*_document_GET.json` is the fetched body (the PATCH request payload);
`*_document_PATCH.json` is the counterparty's response to the PATCH.
`readback_R0_before|R1_between|R2_after_{target,template}.json` — the three read
points, for both the PATCHed resource and the body source.
`component_op_*.xml` — the stored Boomi component XML for each REST operation.
`analysis.json` — the derived verdict, computed only from the artifacts above.

## Fixture provenance
* Live component/operation/connection ids: read back from the account at run time.
* Counterparty semantics and the `modifiedOn` control: `../sandbox-services` @`e25849a`
  (see cap155-e3-patch-counterparty), causally independent of this repo.
* Spec/request shapes: the E1/E2 capture harness frozen before this dispatch
  (`.claude/agent-memory/boomi-qa-tester/harness/lib155e2.py`).
* Every asserted expectation comes from the LIVE platform response or the LIVE
  counterparty readback — never from the implementation under test.

## Limits
The connector document store served ONE document per connector record (the output
document). The PATCH request bytes were therefore not captured directly; identity of
the payload is established transitively, via the GET stage's byte-identical output
document, which is what the send stage transmits.

## Redaction (applied at capture time, before archiving)
Identity fields not needed for this verdict were REMOVED, not masked:
* `account_liveness.json` keeps only account id / status / expiry / licence counts and
  each runtime's id, name, type, status and version. The creator email, tenant hostnames
  and cloud/molecule/instance ids were dropped.
* Boomi stamps `createdBy="<user email>"` / `modifiedBy="<user email>"` into EVERY
  component XML readback; in these captures both attribute values are `[redacted]`.
  This changes only those two attributes — the connector configuration the capture exists
  to evidence (method, path, operation and connection wiring) is untouched.
`SHA256SUMS` was regenerated after redaction, so the digests describe the redacted bytes.
