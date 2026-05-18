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
| `BOOMI_AUTH_HEAL_CORRUPT_CLIENTS` | `true` | When `get_client` hits `InvalidToken`/`ValidationError`, delete the corrupted MongoDB doc so the client can re-register cleanly. | Set to `false` (still logs ERROR, just leaves the row) |

`STORAGE_ENCRYPTION_KEY` now also accepts a comma-separated list of
Fernet keys, newest first. Single-value remains backward compatible.
Multi-value wraps in `MultiFernet` (writes use first; reads accept
any) so key rotation no longer requires dropping any documents.

Example zero-downtime rotation:

```bash
# 1. Generate a new key, then set both old+new on the service
NEW_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
OLD_KEY=$(gcloud secrets versions access latest --secret=storage-encryption-key --project=boomimcp)
gcloud secrets versions add storage-encryption-key \
  --data-file=- --project=boomimcp <<< "${NEW_KEY},${OLD_KEY}"
# 2. Redeploy. Writes now use NEW_KEY, reads accept either.
# 3. After 30 days (RT expiry window), all docs are re-encrypted.
# 4. Drop OLD_KEY: re-add the secret as just NEW_KEY.
```
