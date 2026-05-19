# OAuth Migration Runbook: FastMCP v3.1.1 Cutover

Date: 2026-04-11
Incident: `incident-2026-04-11-boomi-connector/`

## What Happened

The migration from vendored FastMCP to upstream v3.1.1 (commit `d73476e`)
changed the MongoDB collection naming convention. The vendored fork used a
sanitization strategy that appends a hash suffix (e.g.,
`mcp_oauth_proxy_clients-4db71f6a`). Upstream v3.1.1 uses clean hyphenated
names (e.g., `mcp-oauth-proxy-clients`).

All pre-migration OAuth state (30 client registrations, 22 stored tokens,
438 JTI mappings) is in the legacy collections and invisible to the new code.

## Decision

Accept the break as a one-time auth-state reset. Do not rename, merge, or
read from the legacy collections. All users must reconnect their Boomi
connector once to repopulate the upstream hyphenated collections.

## Legacy Collections (read-only, 14-day retention)

| Collection | Documents | Status |
|-----------|-----------|--------|
| `mcp_oauth_proxy_clients-4db71f6a` | 30 | Retained read-only |
| `mcp_upstream_tokens-064b3cac` | 22 | Retained read-only |
| `mcp_jti_mappings-a0131f3f` | 438 | Retained read-only |
| `mcp_authorization_codes-62ca573a` | 3 | Retained read-only |
| `mcp_oauth_transactions-6f3deda7` | 0 | Retained read-only |

Delete these after 2026-04-25 (14 days post-migration).

## New Collections (active)

| Collection | Purpose |
|-----------|---------|
| `mcp-oauth-proxy-clients` | Dynamic client registrations |
| `mcp-upstream-tokens` | Encrypted Google OAuth tokens |
| `mcp-jti-mappings` | JWT ID to upstream token mappings |
| `mcp-authorization-codes` | OAuth authorization codes |
| `mcp-oauth-transactions` | OAuth transaction state |
| `mcp-refresh-tokens` | Refresh token metadata |

## User Recovery: Reconnect Boomi in Claude

1. Open claude.ai Settings > Integrations (or MCP servers)
2. Remove/disconnect the Boomi connector
3. Re-add the Boomi connector (same URL: `https://boomi.renera.ai`)
4. Complete the Google OAuth flow when prompted
5. Retry a simple Boomi tool call (e.g., list profiles)

## Verifying Successful Re-auth

After a user reconnects, check Cloud Run logs:

```bash
# Look for successful token issuance
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="boomi-mcp-server" AND httpRequest.requestUrl=~"/token" AND httpRequest.status=200' \
  --project boomimcp --limit 5 \
  --format="table(timestamp,httpRequest.status,httpRequest.userAgent)"
```

## Verifying Successful Token Refresh

After access token expiry (~1 hour), check that refresh works:

```bash
# Look for /token requests - should be 200, not 401
# Compute the timestamp first (works on both Linux and macOS)
SINCE=$(python3 -c "from datetime import datetime,timedelta,timezone; print((datetime.now(timezone.utc)-timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
gcloud logging read \
  "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"boomi-mcp-server\" AND httpRequest.requestUrl=~\"/token\" AND timestamp>=\"${SINCE}\"" \
  --project boomimcp --limit 20 \
  --format="table(timestamp,httpRequest.status,httpRequest.requestMethod)"
```

With diagnostics enabled (`BOOMI_OAUTH_DIAGNOSTICS=true`), also check for:
- `get_client returned None` -- indicates client not found (legacy state)
- `Token endpoint client auth FAILED` -- shows exact failure reason

## Diagnostic Logging (now default-on)

Diagnostic logging at the three silent 401 boundaries (token-endpoint
client auth, client lookup, encrypted storage GET) is **enabled by
default** in production mode as of 2026-05-18. No env var is needed.

Disable (operators only — leaves a 401 path silent):

```bash
gcloud run services update boomi-mcp-server \
  --region us-central1 \
  --project boomimcp \
  --set-env-vars BOOMI_OAUTH_DIAGNOSTICS_DISABLE=true
```

Re-enable (clear the disable):

```bash
gcloud run services update boomi-mcp-server \
  --region us-central1 \
  --project boomimcp \
  --remove-env-vars BOOMI_OAUTH_DIAGNOSTICS_DISABLE
```

The legacy `BOOMI_OAUTH_DIAGNOSTICS=true` continues to be honored as a
back-compat "on" signal (no-op since the default is already on).
Setting `BOOMI_OAUTH_DIAGNOSTICS=false` explicitly opts out.

## MongoDB Diagnostic Script

To inspect collection state at any time:

```bash
MONGODB_URI=$(gcloud secrets versions access latest --secret=mongodb-uri --project=boomimcp) \
STORAGE_ENCRYPTION_KEY=$(gcloud secrets versions access 2 --secret=storage-encryption-key --project=boomimcp) \
.venv/bin/python scripts/diagnose_oauth_storage.py
```

## Cleanup (after 2026-04-25)

1. Delete legacy collections from MongoDB Atlas
2. Diagnostic logging is now permanent — do NOT remove
   `diagnostic_logging.py` or its server.py call site. The silent 401
   paths it monitors are inherent to the upstream design, not a
   transient migration symptom.

## Token refresh hardening (2026-05-18)

Four env vars introduced to harden the OAuth refresh path against the
three root causes of "MCP becomes inaccessible after ~1h without
session reload" (refresh-token rotation race, silent storage failures,
encryption-key rotation pain).

All four ship enabled by default with safe values. Override only when
debugging or rolling out a key rotation.

| Env var | Default | Purpose | Off-switch |
|---|---|---|---|
| `BOOMI_RT_GRACE_SECONDS` | `60` | Window during which a just-rotated refresh token still returns the same new tokens it produced on first use (defeats one-time-use replay race in clients). | Set to `0` |
| `BOOMI_RT_GRACE_MAX_SIZE` | `512` | LRU capacity for the grace-window cache. | — |
| `BOOMI_OAUTH_DIAGNOSTICS_DISABLE` | unset (= diagnostics ON) | Turn off all OAuth diagnostic logging (the three silent-401 patches in `diagnostic_logging.py`). | Set to `true` |
| `BOOMI_AUTH_HEAL_CORRUPT_CLIENTS` | `true` | When `get_client` hits `DecryptionError`/`DeserializationError` (or the bare `InvalidToken`/`ValidationError` defensive cases), delete the corrupted MongoDB doc so the client can re-register cleanly. | Set to `false` (still logs ERROR, just leaves the row) |

`STORAGE_ENCRYPTION_KEY` now also accepts a comma-separated list of
Fernet keys, newest first. Single-value remains backward compatible.
Multi-value wraps in `MultiFernet`: reads accept any listed key, writes
use the first one.

### Zero-downtime key rotation procedure

**Important caveat.** `MultiFernet` rewraps on PUT, never on GET. The
`mcp-upstream-tokens`, `mcp-jti-mappings`, and `mcp-refresh-tokens`
collections naturally rotate to the new key because token refresh
rewrites those rows on every refresh cycle. But the
`mcp-oauth-proxy-clients` collection holds Dynamic Client Registration
(DCR) documents that are written **once** at registration and never
naturally rewritten by normal traffic. If you drop OLD_KEY before those
docs are re-encrypted, every long-lived client will fail decryption on
its next request and (with `BOOMI_AUTH_HEAL_CORRUPT_CLIENTS=true`)
have its registration deleted -- forcing every user to re-add the
connector. To avoid that, run the rewrap helper before dropping the
old key.

```bash
# 1. Add the new key, leaving the old one in place. Newest first.
NEW_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
OLD_KEY=$(gcloud secrets versions access latest --secret=storage-encryption-key --project=boomimcp)
gcloud secrets versions add storage-encryption-key \
  --data-file=- --project=boomimcp <<< "${NEW_KEY},${OLD_KEY}"

# 2. Redeploy the service. Writes now use NEW_KEY; reads accept either.
#    Token-store rows re-encrypt naturally as refresh traffic flows.

# 3. Re-encrypt the DCR client docs (they are NOT rewritten by refresh).
#    Run with the same env vars the server uses:
MONGODB_URI=$(gcloud secrets versions access latest --secret=mongodb-uri --project=boomimcp) \
STORAGE_ENCRYPTION_KEY="${NEW_KEY},${OLD_KEY}" \
.venv/bin/python scripts/rewrap_oauth_clients.py
# Exit 0 with decrypt_failed=0 is required before step 4.
# Optionally inspect first with `--dry-run`.

# 4. Optional: wait ~30 days for transient token-store rows to roll over
#    naturally (the longest TTL in those collections is the upstream
#    refresh-token expiry). Skip if you trust step 3 + active traffic.

# 5. Drop OLD_KEY: re-add the secret as just NEW_KEY.
gcloud secrets versions add storage-encryption-key \
  --data-file=- --project=boomimcp <<< "${NEW_KEY}"
```

If step 3 reports `decrypt_failed > 0`, do **not** drop the old key.
Either restore the missing historical key, or accept that the listed
clients will need to re-register, then re-run before dropping.

## OAuth cache hardening (2026-05-18, second pass)

Closes the two cache-shaped gaps the original hardening PR left open:

- **Fix B — Google verifier cache.** Caches the `AccessToken` returned
  by `GoogleTokenVerifier.verify_token` in-process so a burst of MCP
  tool calls from one authed session triggers exactly one
  `tokeninfo`+`userinfo` round trip per cache TTL (default 5 min)
  instead of two Google calls per tool call. Transient Google
  rate-limits become cache hits instead of silent 401 cascades.

- **Fix D — Cross-instance grace cache.** Backs the per-process
  refresh-token grace cache (from the first hardening pass) with a
  Fernet-encrypted MongoDB collection so a rotation that lands on
  Cloud Run replica A is visible to replica B during the grace window.
  Closes the multi-instance `Refresh token mapping not found` 401 that
  the per-process cache alone could not prevent.

- **Fix D.2 — Distributed singleflight (opt-in, off by default).**
  When two replicas start refreshing the same RT at exactly the same
  millisecond, the per-replica leader claims a Mongo lock row; the
  losing replica polls the shared cache for the result. Off by default
  because the L2 cache (Fix D) closes most of the race; enable only if
  you observe duplicate `orig_exchange` calls in the diagnostic logs.

### New env vars

All five ship with safe defaults. Override only when debugging or
tuning Google call volume.

| Env var | Default | Purpose | Off-switch |
|---|---|---|---|
| `BOOMI_TOKEN_CACHE_DISABLE` | unset (= cache ON) | Bypass Fix B entirely; every MCP tool call hits Google `tokeninfo`/`userinfo` as before. | Set to `true` |
| `BOOMI_TOKEN_CACHE_TTL_SECONDS` | `300` | Upper bound on per-entry TTL. Caps the window in which a Google-side token revocation is not honored. | Lower to e.g. `60` |
| `BOOMI_TOKEN_CACHE_MAX_SIZE` | `256` | LRU capacity for the verifier cache. | — |
| `BOOMI_TOKEN_CACHE_SWR` | `false` | Opt-in stale-while-revalidate; serves stale within `BOOMI_TOKEN_CACHE_SWR_WINDOW` and refreshes in the background. | Set to `false` |
| `BOOMI_TOKEN_CACHE_SWR_WINDOW` | `30` | Seconds before a cached entry's expiry at which SWR returns stale and schedules a refresh. | — |
| `BOOMI_RT_GRACE_SHARED` | `true` (when `MONGODB_URI` is set) | Enables the Fix D shared MongoDB-backed grace cache (`mcp-rt-grace`). | Set to `false` |
| `BOOMI_RT_GRACE_SHARED_COLLECTION` | `mcp-rt-grace` | Collection name override; unlikely to need outside tests. | — |
| `BOOMI_RT_GRACE_DISTRIBUTED_LOCK` | `false` | Opt-in Fix D.2 cross-instance singleflight via the `mcp-rt-inflight-locks` collection. | Set to `false` |
| `BOOMI_RT_GRACE_LOCK_TTL_SECONDS` | `30` | Safety bound on Fix D.2 lock rows. A crashed leader auto-releases via the collection's TTL index. | — |
| `BOOMI_RT_GRACE_LOCK_POLL_MS` | `100` | Fix D.2 follower polling interval against the shared cache. | — |

### New MongoDB collections

- `mcp-rt-grace` — Fix D shared grace cache. Fernet-encrypted via the
  same `STORAGE_ENCRYPTION_KEY` (MultiFernet-aware) as the rest of
  OAuth state. One row per active old refresh-token hash, auto-evicted
  by the underlying store's TTL handling (~60s default lifetime).
- `mcp-rt-inflight-locks` — Fix D.2 only. One row per in-flight refresh
  across the fleet, auto-evicted via a TTL index on `expires_at` so a
  crashed leader instance does not deadlock subsequent refreshes
  longer than `BOOMI_RT_GRACE_LOCK_TTL_SECONDS`.

Both collections live in the same `boomi_mcp` database. Neither needs
explicit cleanup — they grow proportional to refresh-token rotation
volume and auto-evict via TTL.

### Rollout

Recommended phases (matching `docs/plans/oauth_token_verifier_cache_plan.json`):

1. Merge with defaults (Fix B on, Fix D on, Fix D.2 off). Tail logs
   for a week. Confirm Google `tokeninfo` call rate drops ~92 % and
   the cross-replica `invalid_grant` rate from `mcp-rt-grace` is near
   zero.
2. If `Refresh-token grace SHARED HIT` lines from a replica that did
   not perform the original rotation appear in the diagnostic logs,
   Fix D is working. If duplicate `orig_exchange` calls for the same
   hash within milliseconds appear, set
   `BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true` and redeploy (config-only).

### Rollback

Per-fix off-switches: set `BOOMI_TOKEN_CACHE_DISABLE=true`,
`BOOMI_RT_GRACE_SHARED=false`, or `BOOMI_RT_GRACE_DISTRIBUTED_LOCK=false`.
Full rollback: `git revert <merge_commit>`. The two new MongoDB
collections auto-evict via TTL; explicit cleanup is optional.

## Self-heal circuit-breaker alert (Fix C.2 safety)

### Why

Fix C.2 (`storage_healing_patch.py`, default-on) deletes any
`mcp-oauth-proxy-clients` document that fails decryption /
deserialization so the affected client can re-register cleanly via
DCR. The dangerous failure mode: a misconfigured
`STORAGE_ENCRYPTION_KEY` rotation — e.g., the operator drops OLD_KEY
before `scripts/rewrap_oauth_clients.py` finishes — makes EVERY
long-lived DCR client doc fail decryption, and the heal path silently
deletes them all as each user's client hits the server. The only
operator-visible signal is the ERROR log line
`Corrupted oauth client document detected` — which is only useful if
someone is watching.

This alert is the circuit-breaker: it fires when the deletion rate
exceeds normal-operations baseline, so an operator can flip
`BOOMI_AUTH_HEAL_CORRUPT_CLIENTS=false` before the wave consumes the
whole client registry.

### One-time setup

The alert is provisioned by an idempotent operator helper:

```bash
# Review what would be created without touching GCP:
python scripts/setup_corruption_alert.py --dry-run

# Run for real with a pre-existing notification channel:
python scripts/setup_corruption_alert.py \
  --notification-channel projects/boomimcp/notificationChannels/<channel-id>
```

Defaults: project `boomimcp`, service `boomi-mcp-server`, metric
`boomi-mcp-oauth-client-corruption`, policy
`boomi-mcp-oauth-client-corruption-rate`, threshold **more than 3
deletions in any 300 s rolling window**. Override via
`--threshold`/`--duration-seconds`. Re-runs skip existing resources;
pass `--update` to delete and recreate.

Prerequisite: `gcloud auth login` against an account with
`logging.logMetrics.create`, `monitoring.alertPolicies.create` on
project `boomimcp`. The script fails fast if no active account.

### Response runbook (when the alert fires)

1. **Stop the bleeding immediately.** Flip the kill switch with the
   **additive** flag — `--update-env-vars` adds/overrides the named
   variable while leaving every other env var (`OIDC_*`, `MONGODB_URI`,
   `JWT_SIGNING_KEY`, `STORAGE_ENCRYPTION_KEY`, `SESSION_SECRET`, ...)
   intact. Do **not** use `--set-env-vars` here — it replaces the entire
   env-var set and would take the service down during the incident:
   ```bash
   gcloud run services update boomi-mcp-server \
     --region us-central1 --project boomimcp \
     --update-env-vars BOOMI_AUTH_HEAL_CORRUPT_CLIENTS=false
   ```
   Cloud Run rolls a new revision; further `get_client` failures still
   log ERROR but no longer delete the document.

2. **Identify scope.** Recent deletions:
   ```bash
   SINCE=$(python3 -c "from datetime import datetime,timedelta,timezone; \
     print((datetime.now(timezone.utc)-timedelta(minutes=30)) \
       .strftime('%Y-%m-%dT%H:%M:%SZ'))")
   gcloud logging read \
     "resource.type=\"cloud_run_revision\" \
      AND resource.labels.service_name=\"boomi-mcp-server\" \
      AND severity=ERROR \
      AND textPayload:\"Corrupted oauth client document detected\" \
      AND timestamp>=\"${SINCE}\"" \
     --project boomimcp --limit 50 \
     --format="table(timestamp,textPayload)"
   ```

3. **Inspect MongoDB.** Compare current row count in
   `mcp-oauth-proxy-clients` against the diagnose script's prior
   snapshot:
   ```bash
   MONGODB_URI=$(gcloud secrets versions access latest --secret=mongodb-uri --project=boomimcp) \
   STORAGE_ENCRYPTION_KEY=$(gcloud secrets versions access latest --secret=storage-encryption-key --project=boomimcp) \
     python scripts/diagnose_oauth_storage.py
   ```

4. **Root-cause.** Common triggers in order of likelihood:
   - Key rotation: `STORAGE_ENCRYPTION_KEY` was just changed; the
     rewrap step was skipped. Recover by restoring the old key as
     part of the MultiFernet list and re-running
     `scripts/rewrap_oauth_clients.py`.
   - Bad migration: somebody manually edited the collection in
     MongoDB Atlas, corrupting the BSON value.
   - Single-user bit-rot: only one client_id appears in step 2;
     this is the rare-but-real edge case and Fix C.2 handled it
     correctly. Re-enable the heal in step 5.

5. **Re-enable self-heal once root cause is verified fixed.** Remove
   the kill-switch env var:
   ```bash
   gcloud run services update boomi-mcp-server \
     --region us-central1 --project boomimcp \
     --remove-env-vars BOOMI_AUTH_HEAL_CORRUPT_CLIENTS
   ```

### SLO note

The metric also acts as a low-grade SLO: steady-state value is **zero**.
Any non-zero rate over a 24-hour window indicates either real data
corruption, a botched rotation, or a regression in the storage stack
worth investigating. The alert's 300 s / >3 threshold is the
*circuit-breaker* tier, not the *SLO* tier; long-term zero is the
real target.

## Production enablement of cross-instance grace + logging visibility (2026-05-18, third pass)

### Why

The first-pass cache hardening (PR #34) added the cross-instance
shared grace cache and the per-process Google verifier cache.
`BOOMI_RT_GRACE_SHARED` defaults to `true` in
`rt_grace_shared_backend.initialize_shared_grace_backend`, so Fix D
has actually been active in production since PR #34 — but
`BOOMI_RT_GRACE_DISTRIBUTED_LOCK` defaults to `false`, so the
Fix D.2 singleflight was off, and the four hardening patches log
under the `boomi.*` logger tree, which inherited Python's default
WARNING level — their `INFO` boot lines were silently dropped, so
operators had no log-based way to confirm what was running.

This pass:
- Pins **`BOOMI_RT_GRACE_SHARED=true`** in `cloudbuild.yaml` and
  `k8s/deployment.yaml`. The code default is already `true`; pinning
  makes the manifest reflect the intended state and prevents a
  future default flip from silently turning Fix D off.
- Pins **`BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true`** alongside it.
  This is a real activation — the code default is `false`, so
  before this PR concurrent refreshes across replicas could each
  call Google independently. Pinning engages the Mongo
  upsert-as-lock singleflight so only one replica calls Google per
  refresh-token rotation.
- Adds a scoped `boomi.*` stderr handler at INFO in `server.py`
  (with its handler level pinned to INFO so propagated DEBUG records
  from `boomi.oauth_diagnostic` don't leak past the parent filter)
  so the patches' boot lines reach Cloud Logging without flooding
  it with third-party chatter.

Shared-cache read failures degrade to the local exchange path
(no hard 401 on Mongo outage), so leaving these features on is safe
even with intermittent Mongo connectivity.

### One-shot enable on a running revision

If you want Fix D.2 (distributed lock) on **before** the next Cloud
Build deploy completes, or you want to flip an unset
`BOOMI_RT_GRACE_SHARED` explicitly:

```bash
gcloud run services update boomi-mcp-server \
  --region us-central1 --project boomimcp \
  --update-env-vars BOOMI_RT_GRACE_SHARED=true,BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true
```

`--update-env-vars` is additive — other env vars (`MONGODB_URI`,
`OIDC_CLIENT_*`, `STORAGE_ENCRYPTION_KEY`, etc.) are preserved.

### Expected boot log lines

After the new revision rolls out, you should see one of each per
replica during boot (filter substring → expected line):

| Filter substring | Source | Expected |
|---|---|---|
| `Token verifier cache ENABLED` | `token_cache_patch` | Always (Fix B is on by default) |
| `Refresh-token grace window ENABLED` | `refresh_token_grace_patch` | Always (Fix A is on by default) |
| `Shared grace cache backend ENABLED` | `rt_grace_shared_backend` | Whenever `BOOMI_RT_GRACE_SHARED!=false` (now pinned true) |
| `Distributed grace lock ENABLED` | `rt_grace_shared_backend` | Only when `BOOMI_RT_GRACE_DISTRIBUTED_LOCK=true` (now pinned true) |

Tail a single boot:

```bash
gcloud logging read 'resource.type="cloud_run_revision"
  AND resource.labels.service_name="boomi-mcp-server"
  AND (textPayload:"Shared grace cache backend ENABLED"
    OR textPayload:"Distributed grace lock ENABLED"
    OR textPayload:"Refresh-token grace window ENABLED"
    OR textPayload:"Token verifier cache ENABLED")' \
  --project boomimcp --freshness=1h --limit 10 --order=asc
```

### Rollback

Per-fix off-switches require setting the value **explicitly to
`false`** — `--remove-env-vars` alone is not enough for
`BOOMI_RT_GRACE_SHARED` because its code default is `true` (and so
unsetting it leaves the feature on). `BOOMI_RT_GRACE_DISTRIBUTED_LOCK`
defaults to `false`, so for that one either `--update-env-vars
=false` or `--remove-env-vars` works.

```bash
# Disable Fix D entirely (drops the shared cache back to local-only).
gcloud run services update boomi-mcp-server \
  --region us-central1 --project boomimcp \
  --update-env-vars BOOMI_RT_GRACE_SHARED=false

# Disable Fix D.2 only (keep shared cache, drop the distributed lock).
gcloud run services update boomi-mcp-server \
  --region us-central1 --project boomimcp \
  --update-env-vars BOOMI_RT_GRACE_DISTRIBUTED_LOCK=false
```

The local grace + singleflight from PR #33 still protects each
replica individually after either rollback. The logging change is
pure observability — revert by removing the `_boomi_log` block at
the top of `server.py`.
