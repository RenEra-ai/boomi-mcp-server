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

### Rule 1: NEVER Develop Directly in Main
- All new features MUST be developed in dev branch first
- Test locally using `server_local.py` before any merge
- Main branch is for production-ready, tested code only

### Rule 2: ALWAYS Update Both Server Files
When modifying MCP tools, update in this order:
1. `server_local.py` - implement and test locally
2. `server.py` - copy the exact same changes (excluding auth code)
3. Run sync verification before committing

### Rule 3: Verify Before Every Commit
```bash
python scripts/verify_sync.py
```
This runs automatically via pre-commit hook.

### What Gets Checked
- Function signatures (all parameters)
- CREATE section fields (request_data assignments)
- UPDATE section fields (updates assignments)
- All MCP tools: manage_trading_partner, manage_organization, manage_process

### Setting Up Pre-commit Hook (One-time)
The pre-commit hook is already in `.git/hooks/pre-commit`. If it's not working:
```bash
chmod +x .git/hooks/pre-commit
```

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

## Keeping Server Files in Sync

⚠️ **CRITICAL**: This project has TWO server files that must be kept in sync:

| File | Purpose | Used By |
|------|---------|---------|
| `server.py` | Production server with OAuth | Cloud Run (main branch) |
| `server_local.py` | Local dev server without OAuth | Local testing (dev branch) |

### When Adding/Modifying MCP Tools

**ALWAYS update BOTH files** when:
1. Adding new parameters to an MCP tool function
2. Changing how parameters are passed to action handlers
3. Adding new MCP tools
4. Modifying tool docstrings

### Common Mistake to Avoid

❌ **Wrong**: Update only `server_local.py`, test locally, assume it works in prod
✅ **Right**: Update both `server.py` AND `server_local.py` with identical tool signatures

### Automated Sync Verification

Use the verification script to check all MCP tools at once:

```bash
python scripts/verify_sync.py
```

**Expected output when in sync:**
```
Checking manage_trading_partner...
  Function params: 173 = 173 ✅
  CREATE fields:   170 = 170 ✅
  UPDATE fields:   167 = 167 ✅

Checking manage_organization...
  Function params: 16 = 16 ✅
  CREATE fields:   13 = 13 ✅
  UPDATE fields:   13 = 13 ✅

Checking manage_process...
  Function params: 5 = 5 ✅

All checks passed! ✅
```

**If files are out of sync:**
```
Checking manage_trading_partner...
  Function params: 173 = 173 ✅
  CREATE fields:   170 vs 161 ❌
    Missing in dev: as2_authentication_type, as2_verify_hostname, ...

❌ Files are out of sync!
```

### Checklist Before Merging to Main

1. Run sync verification:
   ```bash
   python scripts/verify_sync.py
   ```

2. If differences found, update the file with fewer fields

3. Commit only when verification passes (pre-commit hook enforces this)

### Why This Matters

- `server_local.py` is used for local testing with `./run_local.sh`
- `server.py` is deployed to Cloud Run for production
- If they diverge, features work locally but fail silently in production
- Parameters not in the function signature are silently ignored by MCP

---

## Selective Merge from Dev to Main

**Purpose**: Merge only specific features (e.g., new MCP tools) from dev to main while preserving main's production-critical code (OAuth, Redis, encryption).

### Key Principle

⚠️ **CRITICAL**: Main branch contains production-critical features that dev branch may not have (Redis token storage, JWT signing, Fernet encryption). Always preserve these when merging.

### Files That Should ONLY Be in Dev Branch

These local development files should **NEVER** be in main:
- `server_local.py` - Local dev server without OAuth
- `run_local.sh` - Script to run local server
- `setup_local.sh` - Local environment setup
- `src/boomi_mcp/local_secrets.py` - Local file-based credential storage

### Selective Merge Process

#### Step 1: Identify Commits to Merge

```bash
# View commits in dev not in main
git log main..dev --oneline

# Identify specific commit hashes for the feature you want to merge
```

#### Step 2: Create Feature Branch from Main

```bash
git checkout main
git checkout -b feature/new-functionality
```

#### Step 3: Cherry-Pick Specific Commits

```bash
# Cherry-pick only the commits you want (replace with actual hashes)
git cherry-pick <commit-hash-1>
git cherry-pick <commit-hash-2>
git cherry-pick <commit-hash-3>

# If conflicts occur, resolve them carefully
# ALWAYS preserve main's OAuth/Redis/JWT/Fernet implementation
git add .
git cherry-pick --continue
```

#### Step 4: Remove Local Dev Files (If Accidentally Included)

```bash
# Check if local dev files were included
ls -la server_local.py run_local.sh setup_local.sh

# If present, remove them
git rm server_local.py run_local.sh setup_local.sh src/boomi_mcp/local_secrets.py
git commit -m "remove local dev files from feature branch"
```

#### Step 5: Verify Critical Systems Preserved

```bash
# Verify OAuth implementation uses GoogleProvider + Redis
grep -A 5 "from fastmcp.server.auth.providers.google import GoogleProvider" server.py
grep -A 3 "RedisStore" server.py

# Verify Redis dependency exists
grep "py-key-value-aio\[redis\]" requirements.txt

# Verify new tools are registered
grep "@mcp.tool()" server.py
```

#### Step 6: Test and Merge

```bash
# Switch to main
git checkout main

# Merge feature branch
git merge feature/new-functionality --no-ff -m "descriptive message"

# Push to trigger deployment
git push origin main
```

#### Step 7: Apply Same Changes to Dev (Optional)

```bash
# If you made improvements during merge (e.g., added annotations)
# cherry-pick them back to dev
git checkout dev
git cherry-pick <commit-hash-from-main>
git push origin dev
```

### Example: Merging Trading Partner Tools

This was successfully completed in November 2025:

```bash
# Created feature branch
git checkout main
git checkout -b feature/add-trading-partner-tools

# Cherry-picked 3 trading partner commits from dev
git cherry-pick c10814f  # Add trading partner management tools
git cherry-pick df7a708  # Complete support for all 7 standards
git cherry-pick c897e91  # Add user prompts

# Removed local dev files that were accidentally included
git rm server_local.py run_local.sh setup_local.sh src/boomi_mcp/local_secrets.py
git commit -m "remove local dev files"

# Added local dev support files back (needed for functionality)
git checkout dev -- src/boomi_mcp/local_secrets.py run_local.sh setup_local.sh
git add src/boomi_mcp/local_secrets.py run_local.sh setup_local.sh
git commit -m "add local development support files"

# WAIT - this was wrong. Removed them again.
git rm server_local.py run_local.sh setup_local.sh src/boomi_mcp/local_secrets.py
git commit -m "remove local dev files - they belong only in dev branch"

# Verified OAuth was preserved
grep "GoogleProvider" server.py | head -5

# Merged to main
git checkout main
git merge feature/add-trading-partner-tools --no-ff
git push origin main
```

### Conflict Resolution Guidelines

When conflicts occur in `server.py`:

**OAuth Section (lines ~88-145)**:
- ✅ KEEP main's: `GoogleProvider`, `RedisStore`, `FernetEncryptionWrapper`, `jwt_signing_key`
- ❌ REJECT dev's: `OAuthProxy`, `GoogleProviderWithRefresh` (simplified version)

**Requirements.txt**:
- ✅ KEEP: `py-key-value-aio[redis]>=0.1.0` (required for OAuth)
- ✅ UPDATE: `fastmcp>=2.13.0` (if dev has newer version)

**Tool Registrations**:
- ✅ ADD: New tool registrations from dev
- ✅ KEEP: Existing annotations (`readOnlyHint`, `openWorldHint`)

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
