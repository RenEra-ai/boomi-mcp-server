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
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="boomi-mcp-server" AND httpRequest.requestUrl=~"/token" AND timestamp>="$(date -u -v-2H +%Y-%m-%dT%H:%M:%SZ)"' \
  --project boomimcp --limit 20 \
  --format="table(timestamp,httpRequest.status,httpRequest.requestMethod)"
```

With diagnostics enabled (`BOOMI_OAUTH_DIAGNOSTICS=true`), also check for:
- `get_client returned None` -- indicates client not found (legacy state)
- `Token endpoint client auth FAILED` -- shows exact failure reason

## Diagnostic Logging

Temporary observability is available via `BOOMI_OAUTH_DIAGNOSTICS=true`.

To enable on Cloud Run:

```bash
gcloud run services update boomi-mcp-server \
  --region us-central1 \
  --project boomimcp \
  --set-env-vars BOOMI_OAUTH_DIAGNOSTICS=true
```

To disable after the cutover is stable:

```bash
gcloud run services update boomi-mcp-server \
  --region us-central1 \
  --project boomimcp \
  --remove-env-vars BOOMI_OAUTH_DIAGNOSTICS
```

Plan: Enable for the first 72 hours post-deploy, then disable.

## MongoDB Diagnostic Script

To inspect collection state at any time:

```bash
MONGODB_URI=$(gcloud secrets versions access latest --secret=mongodb-uri --project=boomimcp) \
STORAGE_ENCRYPTION_KEY=$(gcloud secrets versions access 2 --secret=storage-encryption-key --project=boomimcp) \
.venv/bin/python scripts/diagnose_oauth_storage.py
```

## Cleanup (after 2026-04-25)

1. Delete legacy collections from MongoDB Atlas
2. Remove `BOOMI_OAUTH_DIAGNOSTICS` env var from Cloud Run
3. Optionally remove `diagnostic_logging.py` and its import in `server.py`
