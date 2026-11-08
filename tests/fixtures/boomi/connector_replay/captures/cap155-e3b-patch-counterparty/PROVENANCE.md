# cap155-e3-patch-counterparty — PROVENANCE

## What this proves
The COUNTERPARTY half of the REST PATCH double-execution question, measured directly over
HTTP with no Boomi involvement, so the two platform captures beside it can be read against
a known counterparty contract rather than against an assumption.

* `counterparty_patch_replay.json` — three probes:
  - `P1_identical_patch_replay` — create a client, read, PATCH, read, PATCH the IDENTICAL
    payload again, read. Both writes 200; the business state converges; `modifiedOn` moves
    on the second write; `createdOn` is stable.
  - `CA_no_write_drift_control` — two reads with NO write between them. `modifiedOn` does
    not move. This is what makes a moved `modifiedOn` a genuine write witness rather than
    ambient drift.
  - `CB_different_payload_control` — a PATCH with a DIFFERENT payload. The business digest
    moves. This is what makes "the business digest did not move" a real observation rather
    than a probe that cannot see anything.
* `account_liveness.json` — C1, redacted (see below).

## Provenance of the fixture and of the expectations
Counterparty behaviour and the control field come from `../sandbox-services` @`e25849a` —
its own `rest/app/routers/entities.py` and `shared/store.py`, a repository causally
independent of boomi-mcp-server and of the implementation under test. Facts relied on:
`PATCH <item>` is a shallow-merge upsert; `READONLY_FIELDS = (key, createdOn, createdBy,
modifiedOn, modifiedBy)` are stripped from any caller body, so a client CANNOT set them;
`modifiedOn` is server-set to `now` on EVERY upsert. Every number in the JSON is a live
measurement taken at capture time, not a restatement of that source.

## The C3 control, stated plainly
`modifiedOn` is the control field: a server-managed mutation witness that moves on any
write and only on a write. The sandbox entity carries NO version/etag/sequence counter
(`next_counter` exists in the store but is not applied to these documents), so `modifiedOn`
is the only such field available. It distinguishes "the second write happened and
converged" from "the second write did not happen", which is what the verdict rests on. It
licenses NO claim about write COUNTS: two writes inside the same millisecond would be
indistinguishable from one.

## Redaction
`account_liveness.json` keeps only account id / status / expiry / licence counts and each
runtime's id, name, type, status and version. Creator email, tenant hostnames and
cloud/molecule/instance ids were dropped, not masked. Digests in `records.json` and
`SHA256SUMS` are computed AFTER redaction.
