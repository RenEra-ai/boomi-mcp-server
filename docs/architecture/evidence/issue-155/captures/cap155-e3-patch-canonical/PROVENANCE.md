# cap155-e3-patch-canonical — PROVENANCE

## What this proves
The SAME double-execution question as cap155-e3-patch-legacy, but driven through the
CANONICAL `authoring_request` / `process_ir` surface with first-class `connector_call`
nodes — the route on which `rest.patch` is one of the served callable family/action
pairs, and therefore the route whose retry/idempotency classification is at issue.

Two independent authoring routes, the same counterparty contract, the same verdict.

Account `traininghlibbochkarov-JKIY2X`, runtime `renera-local-atom`, environment
`Local Test Env`, 2026-08-27.

## The verdict (see analysis.json)
* Both executions `COMPLETE`, in=1 out=2, 4 connector rows each, errorCount 0,
  PATCH GenericConnectorRecord `SUCCESS`.
* Identical payload established the same way: GET-stage output document byte-identical
  across runs (`33a00085…`), source resource unmoved at R0/R1/R2.
* Second identical PATCH: business digest unchanged (`5203d544…` at R1 and R2);
  full state NOT identical; exactly ONE differing field, `modifiedOn`
  (run1 `2026-08-27T06:10:41.821Z` -> replay `2026-08-27T06:10:53.636Z`).
* `createdOn` stable; first-PATCH-changed-state positive control fired.

## Authoring facts measured on the way in (served refusals, verbatim codes)
Both were refusals of MY request, not defects — recorded because they are the
served contract for this route:
* A `sequence` mixing a legacy `source` placeholder with a `connector_call` is refused
  `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/intent/units/0/process_ir/body`:
  "a connector_call sequence may not also author the legacy source/target endpoint
  placeholders — author every call as a connector_call".
* `conflict_policy: "fail"` against the pre-existing shared REST connection is refused
  `AUTHORING_COMPILE_BLOCKED` at `/components/conn` ("The component-plan lint marked
  this step unexecutable; apply would refuse it"). The served enum is
  `["reuse", "clone", "fail"]`, default `reuse`.
* With every call authored as a `connector_call` and `conflict_policy: "reuse"`,
  compile succeeded (`LEGACY_PLAN_WARNING` only), apply created the two operations and
  the process and REUSED connection `c4281346-…`, and the process packaged and deployed.

## Files
As for the legacy capture, plus `component_proc.xml` — the process XML the canonical
route actually emitted and the platform stored.

## Fixture provenance
Identical to cap155-e3-patch-legacy. The request envelope shape is the served
`AuthoringRequestV1` / `ProcessIRV1` schema fetched from `get_schema_template` at run
time (ProcessIRV1 schema_hash
`sha256:039549a98f710466fe9677e4e19e18ca205060780133ee4b43bb9fa4b1b3fff7`), not a
remembered shape.

## Limits
Same as the legacy capture: the PATCH request bytes are established transitively via
the GET stage's byte-identical output document, not captured directly.

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
