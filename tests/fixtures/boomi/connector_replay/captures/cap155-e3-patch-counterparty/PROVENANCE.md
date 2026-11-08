# cap155-e3-patch-counterparty — PROVENANCE

## What this proves
The COUNTERPARTY half of the REST PATCH double-execution question, measured directly
over HTTP with no Boomi involvement, so the platform capture next to it can be read
against a known counterparty contract rather than against an assumption.

* `account_liveness.json` — C1. `list_boomi_profiles`, `boomi_account_info` and
  `manage_runtimes` through the public MCP tool boundary at capture time.
  Account `traininghlibbochkarov-JKIY2X` status `active`; licences standard 3/0 and
  standard_test 3/0; runtime `renera-local-atom` (89387bdf-…) `ONLINE`, version 26.07.0.
* `counterparty_patch_replay.json` — three probes:
  - `P1_identical_patch_replay` — create a client, read it, PATCH, read, PATCH the
    IDENTICAL payload again, read. Both writes 200; business state converges;
    `modifiedOn` moves on the second write; `createdOn` is stable.
  - `CA_no_write_drift_control` — two reads with NO write between them. `modifiedOn`
    does not move. This is what makes a moved `modifiedOn` a genuine write witness
    rather than ambient drift.
  - `CB_different_payload_control` — a PATCH with a DIFFERENT payload. The business
    digest moves. This is what makes "the business digest did not move" a real
    observation rather than a probe that cannot see anything.

## Provenance of the fixture and of the expectations
* Counterparty behaviour and the control field come from `../sandbox-services` @`e25849a`
  — its own `rest/app/routers/entities.py` and `shared/store.py`, a repository causally
  independent of boomi-mcp-server and of the implementation under test. Facts relied on:
  `PATCH <item>` is a shallow-merge upsert; `READONLY_FIELDS = (key, createdOn,
  createdBy, modifiedOn, modifiedBy)` are stripped from any caller body, so a client
  CANNOT set them; `modifiedOn` is set to `now` by the server on EVERY upsert.
* Every number in the JSON is a live measurement taken at capture time, not a
  restatement of that source.

## The C3 control, stated plainly
`modifiedOn` is the control field: a server-managed mutation witness that moves on any
write and only on a write. The sandbox entity carries NO version/etag/sequence counter
— `next_counter` exists in the store but is not applied to these documents — so
`modifiedOn` is the only such field available. That is sufficient to distinguish
"the second write happened and converged" from "the second write did not happen",
which is the distinction the verdict rests on. It does NOT license any claim about
write COUNTS: two writes at the same millisecond would be indistinguishable from one.

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
