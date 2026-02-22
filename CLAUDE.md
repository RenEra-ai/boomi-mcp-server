# Boomi MCP Server - Deployment Guide

## Overview

This MCP server provides secure Boomi API access for Claude Code with:
- OAuth 2.0 authentication (Google) with refresh token support
- Long-lived sessions (no auto-disconnect)
- Per-user credential storage (GCP Secret Manager)
- Web UI for credential management
- Cloud-native secret storage with automatic replication
- OAuth consent screen (FastMCP 2.13.0+)

**Live Service**: https://boomi.renera.ai

---

## ⚠️ Development Rules (READ FIRST)

### Rule 1: NEVER Modify Main Directly
- All changes MUST be made in dev branch first, then merged to main
- Never commit or push directly to main — only merge from dev
- Test locally using `server_local.py` before any merge
- Main branch is for production-ready, tested code only
- **NEVER merge dev → main until operability is confirmed via dev branch tests**

### Rule 2: Dev Has Only server_local.py — Merge Manual for Main
- `server.py` does NOT exist on dev — `server_local.py` is the single source of truth
- `CLAUDE.md` exists ONLY on dev — never add it to main
- `src/boomi_mcp/` changes merge automatically with `git merge dev`
- When MCP tool code changes in `server_local.py`, apply substitutions to `server.py` on main after merging (see "Merge Manual" section below)

### Rule 3: Verify Full Sync After Every Merge to Main
After cherry-picking or merging from dev to main, ALWAYS run:
```bash
git diff dev main -- src/boomi_mcp/
```
Cherry-picks and conflict resolutions can silently miss companion changes in other files. Only `local_secrets.py` should differ (dev-only). If any other shared source file differs, copy the dev version to main before pushing.

---

## Branch Workflow

### Main Branch
- **Purpose**: Production-ready code only
- **Deployment**: Automatically deploys to https://boomi.renera.ai via CI/CD
- **Testing**: Full OAuth flow with deployed service

### Dev Branch
- **Purpose**: Development and testing of new features
- **Deployment**: NOT deployed to Cloud Run
- **Testing**: ⚠️ **LOCAL MCP SERVER ONLY** - use `server_local.py` for testing
- **Workflow**:
  ```bash
  # Switch to dev branch
  git checkout dev

  # Test changes using local MCP server
  ./run_local.sh
  # OR
  claude mcp add boomi-local stdio -- python3 /path/to/server_local.py
  ```

**IMPORTANT**: Never push dev branch changes directly to production. Always test locally first on dev branch.

---

## Merge Manual: server_local.py (dev) → server.py (main)

### When is this needed?

- **Shared modules only** (`src/boomi_mcp/` changes): Just `git merge dev` — NO server.py edits needed
- **MCP tool changes** (new tools, changed signatures, changed docstrings): Follow the steps below

### Step-by-step: applying tool changes from server_local.py to server.py

#### Step 1: Merge dev → main
```bash
git checkout main
git merge dev
```

This automatically brings all `src/boomi_mcp/` changes, new files, deleted files.

**If no MCP tool code changed in server_local.py**: you're done. Push and stop.

**If MCP tool code changed**: continue to Step 2.

#### Step 2: Identify what changed
```bash
git diff main~1..main -- server_local.py
```

Look for changes in tool sections: `manage_trading_partner`, `manage_process`, `manage_organization`, `boomi_account_info`, `list_boomi_profiles`, or any NEW tool functions.

#### Step 3: Copy changed tool sections and apply substitutions

For each changed tool, copy the function body from `server_local.py` into `server.py`, then apply:

| Find (server_local.py) | Replace with (server.py) |
|---|---|
| `TEST_USER` | `get_user_subject()` |
| `"for local user:"` | `"by user:"` |
| `"called for local user:"` | `"called by user:"` |
| `"(1 consolidated tool, local)"` | `"(1 consolidated tool)"` |
| `"# --- Trading Partner MCP Tools (Local) ---"` | `"# --- Trading Partner MCP Tools ---"` |
| `"# --- Process MCP Tools (Local) ---"` | `"# --- Process MCP Tools ---"` |
| `"# --- Organization MCP Tools (Local) ---"` | `"# --- Organization MCP Tools ---"` |

#### Step 4: boomi_account_info special handling

This tool has additional differences. Only copy the parts you actually changed (new parameters, new logic), don't replace the entire function. Keep server.py's web portal messages, error messages, and `{subject}` log references.

#### Step 5: Credential tools

- `list_boomi_profiles`: apply `TEST_USER` → `get_user_subject()` substitution only
- `set_boomi_credentials` / `delete_boomi_profile`: **commented out** on main — keep them commented

#### Step 6: Do NOT touch these server.py-only sections

- OAuth setup (GoogleProvider, MongoDBStore, FernetEncryptionWrapper)
- `get_user_subject()` function
- Web UI routes (`/web/login`, `/web/callback`, `/`, `/api/credentials`, etc.)
- `__main__` block (HTTP transport, OAuth endpoint printing)

#### Step 7: Commit and push
```bash
git add server.py
git commit -m "sync server.py MCP tools with dev"
git push origin main
```

### Quick reference: what to do per change type

| Change type | Action |
|---|---|
| New/changed code in `src/boomi_mcp/` | Just `git merge dev` — done |
| New files added (tests, modules) | Just `git merge dev` — done |
| New MCP tool added to server_local.py | Copy tool into server.py, apply substitutions |
| Tool signature/logic changed | Copy changed parts into server.py, apply substitutions |
| boomi_account_info changed | Copy only changed parts, keep web portal messages |
| server_local.py NOT changed | Just `git merge dev` — done |

---

## Merging Dev to Main

### Standard Merge Process

```bash
git checkout main
git merge dev
```

This brings all changes automatically. If MCP tool code changed in `server_local.py`, follow the "Merge Manual" section above to apply substitutions to `server.py`.

### Key Principle

Main branch contains production-critical features (OAuth, GCP Secret Manager, Web UI). Always preserve these when merging. The `server.py` on main has sections that do NOT exist on dev — never delete them.

### Conflict Resolution Guidelines

When merge conflicts say "deleted in HEAD and modified in dev":
- `server_local.py` → resolve by removing from main: `git rm --cached server_local.py`
- `CLAUDE.md` → resolve by removing from main: `git rm --cached CLAUDE.md`
- `server.py` → resolve by keeping main's version: `git checkout HEAD -- server.py`

When conflicts occur in `server.py`:

**OAuth Section**:
- KEEP main's: `GoogleProvider`, `MongoDBStore`, `FernetEncryptionWrapper`
- Never overwrite with dev code

**Tool Registrations**:
- ADD: New tool registrations from dev (with substitutions applied)
- KEEP: Existing annotations (`readOnlyHint`, `openWorldHint`)

### Rollback Plan

If issues arise after merge:

```bash
# Option 1: Revert the merge commit
git checkout main
git revert -m 1 <merge-commit-hash>
git push origin main

# Option 2: Reset to before merge (if not yet pushed)
git reset --hard HEAD~1

# Option 3: Redeploy previous Cloud Run revision
gcloud run services update-traffic boomi-mcp-server \
  --region us-central1 \
  --to-revisions <previous-revision>=100 \
  --project boomimcp
```

---

## Current Deployment Architecture

### Technology Stack
- **Runtime**: Python 3.11
- **Framework**: FastMCP 2.13.0 + FastAPI
- **Authentication**: Google OAuth 2.0 with consent screen
- **Storage**: GCP Secret Manager
- **Hosting**: Google Cloud Run (us-central1)
- **CI/CD**: GitHub Actions → Cloud Build → Cloud Run

### Key Components
1. **OAuth Flow**: PKCE-based authentication with refresh token support for long-lived sessions
2. **Consent Screen**: Built-in FastMCP consent screen (v2.13.0)
3. **Credential Storage**: GCP Secret Manager with per-user secret isolation
4. **Session Management**: SessionMiddleware with persistent SECRET_KEY
5. **Boomi Integration**: Direct API calls via boomi-python SDK
6. **Profile Management**: Up to 10 profiles per user

---

## Deployment Process

### Automated Deployment (Recommended)

**Status**: ✅ Active and working

The repository uses GitHub CI/CD with Cloud Build triggers:

```bash
# Simply push to main branch
git add .
git commit -m "your changes"
git push

# Cloud Build automatically:
# 1. Builds Docker image
# 2. Pushes to Artifact Registry (us-central1)
# 3. Deploys to Cloud Run (us-central1)
# 4. Updates live service at https://boomi.renera.ai
```

**GitHub Repository**: `RenEra-ai/boomi-mcp-server`
**Cloud Build Trigger**: Automatically triggers on push to `main`
**Deployment Region**: `us-central1`

### Manual Deployment (For testing)

```bash
# Navigate to project directory
cd /path/to/boomi-mcp-server

# Build and deploy manually
gcloud builds submit --config cloudbuild.yaml.example

# Or deploy from local Docker image
docker build -t boomi-mcp-server .
docker tag boomi-mcp-server us-central1-docker.pkg.dev/boomimcp/cloud-run-source-deploy/boomi-mcp-server:latest
docker push us-central1-docker.pkg.dev/boomimcp/cloud-run-source-deploy/boomi-mcp-server:latest
```

### Environment Variables

Required for Cloud Run (configured in Cloud Run service):

```bash
# OAuth Configuration (stored in GCP Secret Manager)
OIDC_CLIENT_ID=<from-secret-manager>
OIDC_CLIENT_SECRET=<from-secret-manager>
OIDC_BASE_URL=https://boomi.renera.ai

# Session Secret (CRITICAL: Must be persistent)
SESSION_SECRET=<from-secret-manager>

# Storage Backend
SECRETS_BACKEND=gcp
GCP_PROJECT_ID=boomimcp
```

---

## Local Development

### Fast Local Development (Recommended for Testing New Tools)

**⚠️ USE THIS FOR DEV BRANCH TESTING**

**For rapid iteration without Docker or OAuth:**

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the local development server (stdio mode, no auth)
./run_local.sh

# OR use directly with Claude Code:
claude mcp add boomi-local stdio -- python3 /path/to/boomi-mcp-server/server_local.py

# 3. Store credentials using MCP tool
# Use the set_boomi_credentials tool to add your Boomi credentials
```

**Features:**
- ✅ No OAuth authentication (local testing only)
- ✅ No Docker build required
- ✅ Fast startup (instant reload)
- ✅ Local file-based credential storage (~/.boomi_mcp_local_secrets.json)
- ✅ All MCP tools available: list_boomi_profiles, set_boomi_credentials, delete_boomi_profile, boomi_account_info
- ⚠️ **NOT FOR PRODUCTION** - Use server.py with OAuth for production

**File Structure:**
- `server_local.py` - Simplified server without OAuth
- `src/boomi_mcp/local_secrets.py` - Local file-based credential storage
- `run_local.sh` - Convenience script to run local server

### Full Local Development (With OAuth)

**For testing the full OAuth flow locally:**

```bash
# Install dependencies
pip install -r requirements.txt -r requirements-cloud.txt

# Set environment variables
export OIDC_CLIENT_ID="your-client-id"
export OIDC_CLIENT_SECRET="your-client-secret"
export OIDC_BASE_URL="http://localhost:8080"
export SESSION_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SECRETS_BACKEND=gcp
export GCP_PROJECT_ID=boomimcp

# Run server
python server_http.py
```

### Docker Local Testing

```bash
# Build image
docker build -t boomi-mcp-server .

# Run locally
docker run -p 8080:8080 \
  -e OIDC_CLIENT_ID="your-client-id" \
  -e OIDC_CLIENT_SECRET="your-client-secret" \
  -e OIDC_BASE_URL="http://localhost:8080" \
  -e SESSION_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')" \
  boomi-mcp-server

# Test at http://localhost:8080
```

---

## Architecture Details

### File Structure
```
boomi-mcp-server/
├── server.py              # Core MCP server logic with OAuth
├── server_http.py         # HTTP wrapper with SessionMiddleware
├── src/boomi_mcp/
│   ├── cloud_auth.py      # OAuth providers
│   └── cloud_secrets.py   # Storage backends (GCP/AWS/Azure)
├── templates/
│   ├── credentials.html   # Web UI for credential management
│   └── login.html         # OAuth login page
├── static/
│   └── favicon.png        # RenEra logo
├── Dockerfile             # Multi-stage build
├── requirements.txt       # Core dependencies (FastMCP 2.13.0)
└── requirements-cloud.txt # Cloud provider SDKs
```

### Data Flow
1. User visits https://boomi.renera.ai → OAuth login (Google)
2. After auth, can save Boomi credentials (up to 10 profiles) → GCP Secret Manager
3. MCP client connects → OAuth consent screen → approval
4. MCP tools access saved credentials for API calls
5. Results returned to Claude Code

### Current Features
- ✅ FastMCP 2.13.0 with OAuth consent screen
- ✅ OAuth refresh tokens for long-lived sessions (no auto-disconnect)
- ✅ Multiple profile support (up to 10 per user)
- ✅ Profile name is required (no default profile)
- ✅ Email field for Boomi username (auto-prepends BOOMI_TOKEN.)
- ✅ Credential validation before saving
- ✅ Loading states and error messaging
- ✅ MCP server URL display with copy button
- ✅ RenEra favicon

---

## Security Considerations

### Current Implementation
- ✅ OAuth 2.0 with PKCE
- ✅ OAuth refresh tokens (automatic session renewal, no disconnects)
- ✅ OAuth consent screen (prevents confused deputy attacks)
- ✅ Per-user credential isolation
- ✅ HTTPS-only (enforced by Cloud Run)
- ✅ Persistent sessions with cryptographic signing
- ✅ GCP Secret Manager for credential storage
- ✅ No credentials in environment variables (except OAuth keys)
- ✅ Profile limit (max 10 per user)
- ✅ Credential validation before storage

### Future Enhancements
- 🔄 Implement rate limiting
- 🔄 Add audit logging
- 🔄 Credential rotation reminders
- 🔄 Server branding (icons, site) when FastMCP 2.14.0+ is released

---

## GCP Secret Manager

**Status**: ✅ Active (deployed and working)

User credentials are stored as secrets in GCP Secret Manager:
- Format: `boomi-mcp-{user-id}-{profile-name}`
- Examples:
  - `boomi-mcp-glebuar-at-gmail-com-production`
  - `boomi-mcp-103626527684412850515-sandbox`
  - `boomi-mcp-117411948315103061876-dev`

**Benefits:**
- Automatic replication across regions
- Built-in access auditing
- No volume management required
- Instant updates (no restart needed)

**Configuration:**
```bash
# Already configured in Cloud Run
SECRETS_BACKEND=gcp
GCP_PROJECT_ID=boomimcp
```

---

## Monitoring & Logs

```bash
# View recent logs
gcloud run services logs read boomi-mcp-server --region us-central1 --limit 50 --project boomimcp

# Follow logs in real-time
gcloud run services logs tail boomi-mcp-server --region us-central1 --project boomimcp

# Check service status
gcloud run services describe boomi-mcp-server --region us-central1 --project boomimcp

# Check recent builds
gcloud builds list --limit 5 --project boomimcp
```

---

## Troubleshooting

### Issue 1: "Invalid state" Error in OAuth Flow

**Symptom**: Users get "Invalid state" error after OAuth callback
**Cause**: SESSION_SECRET is not persistent (regenerated on each deployment)
**Status**: ✅ Fixed - SESSION_SECRET stored in GCP Secret Manager

### Issue 2: Credentials Not Persisting

**Status**: ✅ No longer applicable - using GCP Secret Manager which persists automatically

### Issue 3: Service Won't Start

**Symptom**: Cloud Run deployment times out
**Cause**: Usually missing environment variables or dependencies

**Solution**:
```bash
# Check logs
gcloud run services logs read boomi-mcp-server --region us-central1 --limit 50 --project boomimcp

# Common fixes:
# - Verify all environment variables are set
# - Check Docker build completed successfully
# - Ensure port 8080 is exposed
# - Verify FastMCP version compatibility
```

### Issue 4: OAuth Consent Screen Not Showing Branding

**Symptom**: Consent screen doesn't show custom logo/icon
**Cause**: FastMCP 2.14.0+ required for branding parameters (icons, site)
**Status**: ⏳ Waiting for FastMCP 2.14.0 release (currently on 2.13.0)

**Temporary**: Server name "Boomi MCP Server" is displayed on consent screen

---

## Updating OAuth Credentials

If OAuth credentials change:

1. Update secrets in GCP Secret Manager:
```bash
# Update client ID
echo -n "NEW-CLIENT-ID" | gcloud secrets versions add oidc-client-id --data-file=- --project boomimcp

# Update client secret
echo -n "NEW-CLIENT-SECRET" | gcloud secrets versions add oidc-client-secret --data-file=- --project boomimcp
```

2. Restart Cloud Run service (picks up new secrets automatically):
```bash
gcloud run services update boomi-mcp-server --region us-central1 --project boomimcp
```

---

## Rollback Process

If deployment fails:

```bash
# List revisions
gcloud run revisions list --service boomi-mcp-server --region us-central1 --project boomimcp

# Route traffic to previous revision
gcloud run services update-traffic boomi-mcp-server \
  --region us-central1 \
  --to-revisions boomi-mcp-server-00034-lhn=100 \
  --project boomimcp
```

---

## CI/CD Configuration

### Current Setup

**Status**: ✅ Fully automated via GitHub

**GitHub Repository**: `RenEra-ai/boomi-mcp-server`
**Branch**: `main`
**Trigger**: Push to main branch
**Build**: Google Cloud Build
**Deploy**: Cloud Run (us-central1)

### How It Works

1. Developer pushes to `main` branch
2. GitHub webhook triggers Cloud Build
3. Cloud Build:
   - Builds Docker image from Dockerfile
   - Pushes image to Artifact Registry (us-central1)
   - Deploys to Cloud Run service
4. Service automatically updates at https://boomi.renera.ai

### Environment Variables (Cloud Run)

Configured in Cloud Run service using GCP Secret Manager:
- `OIDC_CLIENT_ID` → from secret `oidc-client-id`
- `OIDC_CLIENT_SECRET` → from secret `oidc-client-secret`
- `OIDC_BASE_URL` → `https://boomi.renera.ai`
- `SESSION_SECRET` → from secret `session-secret`
- `SECRETS_BACKEND` → `gcp`
- `GCP_PROJECT_ID` → `boomimcp`

### Monitoring Deployments

```bash
# Watch latest build
gcloud builds list --limit 1 --project boomimcp

# View build logs (replace BUILD_ID)
gcloud builds log BUILD_ID --project boomimcp

# Check current revision
gcloud run services describe boomi-mcp-server --region us-central1 --project boomimcp
```

---

## Testing the MCP Server

### From Claude Code

```bash
# Add MCP server
claude mcp add --transport http boomi https://boomi.renera.ai/mcp

# Test connection (triggers OAuth consent screen)
# 1. Browser opens for Google login
# 2. Consent screen shows "Boomi MCP Server"
# 3. Click "Approve"
# 4. Connection established
```

### Available MCP Tools

1. **`boomi_account_info(profile: str)`**
   - Get Boomi account information from a specific profile
   - Profile name is REQUIRED (no default)
   - Returns account details from Boomi API

2. **`list_boomi_profiles()`**
   - List all saved Boomi credential profiles
   - Shows profile names for the authenticated user

---

## Support & Resources

- **Live Service**: https://boomi.renera.ai
- **Web UI**: https://boomi.renera.ai/ (credential management)
- **MCP Endpoint**: https://boomi.renera.ai/mcp
- **GitHub Repository**: https://github.com/RenEra-ai/boomi-mcp-server
- **Cloud Run Logs**: https://console.cloud.google.com/run/detail/us-central1/boomi-mcp-server/logs
- **Cloud Build History**: https://console.cloud.google.com/cloud-build/builds?project=boomimcp

---

## Version History

### Current State (2025-10-28)
- **FastMCP Version**: 2.13.0
- **Deployment**: Cloud Run (us-central1)
- **CI/CD**: ✅ Automated via GitHub
- **Latest Revision**: boomi-mcp-server-00034-lhn
- **Status**: ✅ Production (stable)
- **URL**: https://boomi.renera.ai

### Recent Changes
- ✅ Refactored trading partner tools to align with boomi-python SDK examples (dev branch)
  - Uses XML-based Component API instead of trading_partner_component API
  - Added comprehensive XML builders for all standards (X12, EDIFACT, HL7, RosettaNet, Custom)
  - Migrated to typed query models for list operations
  - All functions now use `id_` attribute pattern from SDK
- ✅ Established dev/main branch workflow for testing
  - Dev branch: Local testing only with server_local.py
  - Main branch: Automatic deployment to production
- ✅ Enabled OAuth refresh tokens for long-lived sessions (no auto-disconnect)
- ✅ Switched to BoomiOAuthProvider for better OAuth control
- ✅ Migrated to GitHub-based CI/CD
- ✅ Updated to FastMCP 2.13.0 (OAuth consent screen)
- ✅ Removed "default" profile concept (profile name now required)
- ✅ Added profile limit (10 per user)
- ✅ Added credential validation before saving
- ✅ Added MCP server URL display with copy button
- ✅ Improved UX with loading states
- ✅ Custom RenEra favicon

### Pending
- ⏳ Server branding (icons, site) - waiting for FastMCP 2.14.0+

---

## Last Updated

- **Date**: 2025-10-28
- **Status**: ✅ Production (stable)
- **CI/CD**: ✅ Fully automated via GitHub
- **Deployment**: Automatic on push to main
