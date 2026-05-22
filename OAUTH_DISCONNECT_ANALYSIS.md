# Boomi MCP — OAuth disconnect investigation (for Codex review)

**Date:** 2026-05-22
**Author:** Claude Code investigation
**Status:** Root cause narrowed to a leading hypothesis + named alternatives. Hand-off for Codex deep analysis before any code change.

---

## 0. How to read this document

This is a self-contained brief. It records a live investigation of why the
Boomi MCP server (`https://boomi.renera.ai`, Cloud Run service
`boomi-mcp-server`, project `boomimcp`, region `us-central1`) keeps
**disconnecting from Claude Code** "after a day or so", forcing the user to
re-authenticate.

Everything below is backed by: the live Cloud Run logs, a read-only inspection
of the production MongoDB Atlas token store, a live reproduction against the
`/token` endpoint, and a read of the FastMCP 3.1.1 source plus the repo's
monkey-patches.

**Important correction:** an earlier pass concluded the cause was refresh-token
TTL expiry (FastMCP's 30-day frozen window). The MongoDB inspection
**disproved that** for this incident — Claude Code's recent refresh-token rows
are all present and healthy. The 30-day window is still a real *latent* bug
(§6.3) but it is not what is disconnecting the user now.

---

## 1. Environment

- **FastMCP** `3.1.1`, `mcp` SDK, `GoogleProvider` (OAuth proxy pattern).
  The server is an OAuth *proxy*: it issues its own HS256 JWT access/refresh
  tokens to MCP clients and brokers Google underneath.
- **Token store:** MongoDB Atlas, DB `boomi_mcp`, via
  `key_value.aio` `MongoDBStore` → `FernetEncryptionWrapper` → `VerifiedStorage`.
  Encryption key `STORAGE_ENCRYPTION_KEY` (Secret Manager `storage-encryption-key`,
  pinned to version `2`), parsed as a comma-separated `MultiFernet`.
- **Cloud Run:** `maxScale: 20`, `containerConcurrency: 80`, `cpu: 1`,
  `memory: 2Gi`, **no `min-instances`**, VPC connector, last deploy
  `2026-05-19T00:17:27Z` (revision `boomi-mcp-server-00210`).
- **Repo monkey-patches** applied at startup (`server.py`):
  `consent_csp_patch`, `loopback_redirect_patch`, `token_cache_patch`,
  `diagnostic_logging`, `storage_healing_patch`, and the OAuth-hardening set
  `refresh_token_grace_patch` + `rt_grace_shared_backend` (PRs #33–#39).
- **Clients seen hitting the server:** Claude Code CLI (`Bun/1.3.14` for
  `/token`, `claude-code/2.1.x (cli)` for `/mcp`; uses **CIMD** —
  `client_id = https://claude.ai/oauth/claude-code-client-metadata`); a
  high-traffic `node`/`undici` client (uses **DCR**); Cursor; `python-httpx`
  (tests/dev).

---

## 2. The confirmed failure

At **2026-05-22 18:31:17 UTC** Claude Code's token refresh failed:

```
18:31:16.948  Bun/1.3.14            GET  /.well-known/oauth-authorization-server  200
18:31:17.071  Bun/1.3.14            POST /token                                   401   ← refresh failed
18:31:17.196  (server)              CIMD document fetched and validated (claude-code-client-metadata)
18:31:17.375  claude-code/2.1.148   POST /mcp                                      401   ← invalid_token
18:31:17.46–.55 claude-code/2.1.148 GET  /.well-known/*                            200   ← re-auth begins
```

`Bun` and `claude-code (cli)` are two sides of the **same** Claude Code client
(Claude Code 2.x is a Bun-compiled binary; `/token` calls report the Bun
default UA, `/mcp` calls report the `claude-code` UA).

**Reproduction (live, harmless):** POSTing a bogus refresh token to the real
endpoint returns the exact failure shape:

```
POST /token  grant_type=refresh_token  refresh_token=<bogus>  client_id=<CC CIMD url>
→ HTTP 401  {"error":"invalid_grant","error_description":"refresh token does not exist"}
```

The 18:31 failure produced **no** application log line other than
"CIMD document fetched and validated". The diagnostic patches in
`diagnostic_logging.py` *do* work (a separate repro with a bad `client_id`
correctly logged `get_client returned None`). Their silence at 18:31 rules out:
client-auth failure, `get_client` miss, and storage decryption error.

**Therefore the 18:31 failure is the `load_refresh_token → None` path**
(`mcp` SDK token handler → `invalid_grant` "refresh token does not exist").
The refresh token Claude Code presented was **not found** in the
`mcp-refresh-tokens` collection.

---

## 3. The decisive evidence — MongoDB inspection

Read-only inspection of DB `boomi_mcp` (no writes; only `client_id` and
timestamp metadata decrypted — no token secrets extracted).

### 3.1 Claude Code's refresh-token rows are all present and healthy

`mcp-refresh-tokens` — the 5 rows for the Claude Code CIMD client:

| created (UTC)        | expires (UTC)        | source        |
|----------------------|----------------------|---------------|
| 2026-05-18 17:11:06  | 2026-06-16 18:28:07  | refresh rotation |
| 2026-05-18 21:13:58  | 2026-06-17 17:57:48  | refresh rotation |
| 2026-05-18 22:21:12  | 2026-06-17 21:14:21  | refresh rotation |
| 2026-05-18 23:21:22  | 2026-06-17 22:23:36  | refresh rotation |
| 2026-05-19 01:19:53  | 2026-06-17 23:21:52  | refresh rotation |

All 5 expire **mid-June 2026** — none expired. `_hash_token` is plain
`hashlib.sha256(token).hexdigest()` (deterministic), and there is exactly one
`mcp-refresh-tokens` collection with consistent get/put routing — so a lookup
**cannot** miss a row that exists.

**Conclusion: the token Claude Code presented at 18:31 was NOT any of these 5
surviving rows — it presented a token whose `mcp-refresh-tokens` row is gone
(rotated away and deleted, or from a chain that has since aged out).**

### 3.2 Claude Code *has* refreshed successfully (correction)

An earlier draft of this section claimed Claude Code had never completed a
successful refresh, inferred from each `mcp-refresh-tokens` row having a
same-timestamp `mcp-upstream-tokens` row. **That inference is invalid:** the
`key_value` `MongoDBStore` **resets a document's `created_at` on every
update**, so an upstream-token row's `created_at` tracks its *last refresh*,
not the original `authorization_code` grant — the timestamp pairing proves
nothing.

The FastMCP rotation log is decisive: `proxy.py:1353` "Issued new FastMCP
tokens (rotated refresh)" fired for the Claude Code client at 05-18 17:11,
21:13, 22:21, 23:21 and 05-19 00:19, 01:19. **Claude Code rotates refresh
tokens successfully and routinely.** The disconnect is therefore a *desync*,
not a blanket refresh failure: rotations succeed server-side, but the client
intermittently ends up holding a token the server has already superseded.

This *strengthens* the §8 durable-reuse-window fix: because rotations do
succeed, a lost/superseded token reliably has a server-side successor to
alias it to.

### 3.3 Other clients refresh fine

The `node`/`undici` DCR client refreshes ~hourly and succeeds (rotation log
`proxy.py:1353` "Issued new FastMCP tokens (rotated refresh)" every hour
05-15→05-19; a successful `/token` 200 at 05-22 18:06). Its chain keeps
exactly **one** refresh-token row (rotation delete works for it).

So the refresh path is **not globally broken** — it is an intermittent desync
that strands the client on a superseded token.

### 3.4 Collection cruft (secondary finding)

`db.list_collection_names()` shows **two generations** of collections:

- Current (hyphenated): `mcp-refresh-tokens` (12), `mcp-jti-mappings` (13),
  `mcp-upstream-tokens` (10), `mcp-oauth-proxy-clients` (11),
  `mcp-rt-inflight-locks` (0), `mcp-authorization-codes` (0).
- Orphaned (underscore + hash suffix), from an older `key_value`/naming
  scheme: `mcp_jti_mappings-a0131f3f` (**438 docs**),
  `mcp_oauth_proxy_clients-4db71f6a` (30), `mcp_upstream_tokens-064b3cac` (22),
  `mcp_authorization_codes-62ca573a` (3), `mcp_oauth_transactions-6f3deda7` (0).
- A stray `default_collection` (0 docs) — see §5.2.

`mcp-rt-grace` (the shared grace cache's intended collection) **does not
exist at all** — see §5.2.

One-time-use rotation delete is also **inconsistent**: `node` keeps exactly 1
refresh-token row, but DCR client `3737285f` has 3 — where one-time-use
rotation should leave only the latest. Worth Codex digging.

---

## 4. Root cause — the mechanism

**The disconnect is a client/server refresh-token desync, amplified by
FastMCP one-time-use rotation, and NOT caught by the grace window because the
grace window is non-functional across restarts.**

Step by step:

1. Claude Code refreshes (or completes an OAuth flow) → server mints + stores
   `{access_token (1h), refresh_token R}`, returns them in the `POST /token`
   HTTP response.
2. **The response is lost / not persisted client-side.** The server has
   committed its side (R's row written); Claude Code does not end up holding R.
3. Claude Code's locally-stored refresh token stays at some **older** value.
4. Next refresh: Claude Code presents the old token →
   `load_refresh_token → None` → `401 invalid_grant "refresh token does not
   exist"` → `/mcp` 401 → disconnect → forced re-auth.
5. The forced re-auth's `/token` response is *also* prone to step 2, and so is
   the next refresh → the disconnect recurs. (Rotations themselves succeed —
   see §3.2 — so each lost response strands the client one token behind.)

### Why responses get lost — the trigger (leading hypothesis)

The Cloud Run service is **unstable at the serving layer**:

- It starts a fresh container instance **~20×/day** (startup banner /
  `Refresh-token grace window ENABLED` logged ~20× on 2026-05-22). With **no
  `min-instances`** and `maxScale: 20`, this is cold starts + autoscale churn.
  (No crashes — zero `severity>=ERROR` in 3 days, no OOM.)
- The logs are full of
  `"Truncated response body. Usually implies that the request timed out or the
  application exited before the response was finished."` — multiple per minute
  during active periods.

When the instance handling a `/token` exchange is recycled (or the response is
otherwise truncated) **after** the server has written its token rows but
**before** the client receives the body, client and server desync. With
one-time-use rotation, that desync is permanent for that token.

⚠️ **This is the leading hypothesis, not yet proven.** "Truncated response
body" on an MCP server is *often* benign SSE-stream teardown on the `/mcp` GET
endpoint. Whether any truncations land on **`POST /token`** responses is the
key open question (§7).

---

## 5. Why the existing OAuth hardening does NOT prevent this

PRs #33–#39 added a refresh-token grace window precisely to absorb
"client re-presents a just-rotated token". It does not help here:

### 5.1 Grace window is 60 s and L1 is per-process

`refresh_token_grace_patch.py`: `BOOMI_RT_GRACE_SECONDS` default **60**. The L1
cache is a per-process `OrderedDict` — **wiped on every container restart**.
Restarts are *the very event* that causes the truncation. So at the client's
retry the new instance's L1 is empty. And 60 s cannot help a client that
reconnects minutes/hours/days later.

### 5.2 L2 shared grace cache (Fix D) is misconfigured

`rt_grace_shared_backend.py` builds `MongoDBStore(coll_name="mcp-rt-grace")`,
but `SharedGraceBackend.get/put/delete` call the store **without** a
per-operation `collection=` argument. In this `key_value` `MongoDBStore`
version, routing is by the per-call `collection` (falling back to a collection
literally named `default_collection`), **not** by the constructor `coll_name`.

Evidence: there is **no `mcp-rt-grace` collection** in the DB; there **is** a
`default_collection`. So the shared grace cache is reading/writing
`default_collection`. Get and put are at least consistent (both unrouted), so
it may *function* — but it is writing to an unintended, shared collection and
the wiring is clearly wrong. **Needs Codex verification that L2 actually
round-trips at all.**

### 5.3 L2 distributed lock (Fix D.2) is dead

`initialize_shared_grace_backend` creates `AsyncIOMotorClient(mongodb_uri)` at
**server startup**, binding it to the startup event loop. The lock calls run
later inside uvicorn's request loop. Confirmed firing in production
(18:06:25 and 19:06:36 on 2026-05-22):

```
[WARNING] boomi.rt_grace_shared  try_claim_lock fallthrough … RuntimeError:
  Task <…RequestResponseCycle.run_asgi()…> got Future <…> attached to a
  different loop
[WARNING] boomi.rt_grace_shared  release_lock failed … (same RuntimeError)
```

`try_claim_lock` swallows it and returns `True` (degrade-to-leader), so every
instance believes it is sole leader. Fix D.2 is 100% inert.

---

## 6. Confirmed / secondary bugs (independent of the disconnect)

### 6.1 Motor event-loop bug — §5.3. Real; fix or disable.

### 6.2 Shared grace cache collection misrouting — §5.2. Real; fix wiring.

### 6.3 FastMCP 30-day frozen refresh window (latent)

`proxy.py`: `refresh_token_expires_at` is set once at the initial
`authorization_code` grant to `now + 30 days` (Google sends no
`refresh_expires_in`; default at `proxy.py:943`). The code that should slide
it forward (`proxy.py:1241-1260`) only runs when **Google rotates its upstream
refresh token in a refresh response** — which Google does not do — so it is
dead code. Every rotated refresh token inherits
`ttl = (initial_consent + 30d) − now`, which shrinks to zero. **After 30 days
from the initial browser consent, all refreshes fail regardless of client
behavior.** Confirmed in the DB: two rows in one chain share an identical
`expires_at` with different `created_at`. Not the current cause, but it *will*
bite. Fix: slide the window on each refresh.

### 6.4 Storage-collection cruft / migration debris — §3.4

438 orphaned JTI docs etc. + a stray `default_collection`. Dead weight and a
foot-gun; needs a deliberate migration/cleanup decision.

### 6.5 Inconsistent one-time-use delete — §3.4

Some chains accumulate undeleted refresh-token rows. Either harmless
(authz-code rows never refreshed) or a real delete bug — Codex to determine.

### 6.6 Silent `/token` failures

The proxy logs refresh-grant failures at **DEBUG** (`proxy.py:1161`,
suppressed in prod). Operationally this OAuth path is near-blind. Add
INFO/WARNING logging of the `invalid_grant` reason.

---

## 7. Open questions for Codex

1. **Why does Claude Code end up holding a refresh token older than the one
   the server last issued it?** Rank/confirm:
   - (a) `POST /token` response truncation due to Cloud Run instance recycling
     (leading hypothesis). *Confirm by correlating "Truncated response body"
     warnings with `/token` POST requests.*
   - (b) A CIMD-specific issue: does the refresh/authorization-code path behave
     differently for a URL `client_id` (CIMD) vs a DCR `client_id`? Note
     `get_client` re-`put`s the CIMD client doc on every call (`proxy.py:639`).
   - (c) A client-side bug in Claude Code 2.x persisting the rotated token
     (out of our control, but must be ruled in/out).
   - (d) Interaction of `loopback_redirect_patch.py` / `consent_csp_patch.py`
     with Claude Code's loopback OAuth completion.
   - (e) Cross-instance one-time-use rotation racing the (non-functional)
     grace layer.
2. Is the L2 shared grace cache (`default_collection` routing) actually
   round-tripping, or silently dead like the lock?
3. Why is one-time-use delete inconsistent across chains (§3.4 / §6.5)?
4. Are the orphaned underscore+hash collections safe to drop?

**Recommended instrumentation to resolve #1 (do before any fix):**
- Promote the `/token` failure reason from DEBUG → INFO/WARNING in the proxy
  (monkey-patch `exchange_refresh_token` / the token handler).
- Tag "Truncated response body" warnings with the request path.
- One-shot: read Claude Code's locally stored Boomi-MCP refresh token,
  `sha256` it, and check membership in `mcp-refresh-tokens` — this directly
  proves/refutes "stale client token".

---

## 8. Proposed solutions (for Codex to evaluate — not yet implemented)

Grouped by intent. Each needs the repo's QA loop (boomi-qa-tester) per
`CLAUDE.md`.

### Area A — Stop losing `/token` responses (the trigger)
- **A1.** Set Cloud Run `min-instances: 1` (or `2`) — keeps a warm instance,
  cuts cold-start churn dramatically. Low risk, immediate.
- **A2.** Investigate the "Truncated response body" warnings: confirm whether
  any are on `POST /token`; check Cloud Run request timeout, CPU
  always-allocated (no throttling), and graceful-shutdown handling so in-flight
  responses finish before an instance is reaped.

### Area B — Make the safety net actually work
- **B1.** Fix the L2 shared grace cache collection routing
  (`rt_grace_shared_backend.py`): pass `default_collection=` to `MongoDBStore`
  or `collection=` on every `get/put/delete`. Verify round-trip.
- **B2.** Fix the Motor event-loop bug: create `AsyncIOMotorClient` lazily on
  first use inside the serving loop (not at startup); or drop the raw-motor
  lock and reuse the `key_value` async stack. Interim: set
  `BOOMI_RT_GRACE_DISTRIBUTED_LOCK=false` (already effectively off — stops the
  WARNING spam).

### Area C — Tolerate desync (the real robustness fix)
A lost `/token` response must not permanently brick the client. Options
(Codex to choose / combine):
- **C1.** Durable, long-lived grace: persist, per old-RT hash, the rotated
  result until the *new* RT is itself used or expires (Auth0-style "refresh
  token reuse interval"), not just 60 s. Survives restarts and long gaps.
- **C2.** Disable FastMCP one-time-use rotation: do not delete the old RT row
  on rotation; let the old RT keep working until it expires. Simplest;
  trades the one-time-use security property for robustness. (Implement as a
  patch on `exchange_refresh_token`.)
- **C3.** Recovery path: in `exchange_refresh_token`, if the RT-hash row is
  missing but the presented JWT still verifies and its JTI mapping +
  upstream-token row still exist, accept it and re-mint instead of 401.

### Area D — Latent / hygiene
- **D1.** Slide the 30-day refresh window: re-stamp
  `refresh_token_expires_at = now + 30d` on every successful refresh
  (§6.3). Patch alongside `refresh_token_grace_patch.py`.
- **D2.** Decide on the orphaned-collection cleanup / `key_value` naming
  migration (§3.4).
- **D3.** Promote OAuth `/token` failure logging DEBUG → INFO (§6.6).

**Suggested order:** instrumentation (§7) → A1 → confirm hypothesis →
C1 or C2 (the durable fix) → B1/B2 → D.

---

## 9. What ChatGPT got right / wrong (the user also asked to verify this)

- **Right:** the Motor distributed-lock is broken with an event-loop error
  (confirmed firing); it is not a fixed 1-day expiry; the problem is
  refresh-token related; the lock should be fixed/disabled.
- **Right in spirit:** "stale client-side token state" — the client does end
  up presenting a stale token. But ChatGPT's *mechanism* (a sibling process
  rotated the token out from under a cached copy) is **not supported**: the
  `node` client and Claude Code are separate clients with separate token
  chains (DCR vs CIMD; cross-client use is blocked at `proxy.py:1126`).
  ChatGPT tied `node`'s 18:06 success to Claude Code's 18:31 failure — they
  are unrelated. The staleness here comes from **lost `/token` responses +
  one-time-use rotation + a non-functional grace window**, not a rotation race.
- **Not a fixed-interval expiry:** correct — and note the separate latent
  30-day frozen-window bug (§6.3) is also real but is not this incident.
