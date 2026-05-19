# Boomi MCP Server

**Secure MCP server for Boomi Platform API integration with Claude Code**

A production-ready Model Context Protocol (MCP) server that enables Claude Code and other MCP clients to interact with Boomi Platform APIs using OAuth 2.0 authentication and secure credential storage.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)
![FastMCP](https://img.shields.io/badge/FastMCP-3.1.1-green.svg)

**🌐 Live Service**: [https://boomi.renera.ai](https://boomi.renera.ai)

---

## Features

- 🔐 **Google OAuth 2.0** - Secure authentication with consent screen
- 🔒 **GCP Secret Manager** - Encrypted per-user credential storage
- 👤 **Multi-Profile Support** - Store up to 10 Boomi account profiles per user
- 🌐 **Web UI** - Browser-based credential management
- ✅ **Credential Validation** - Test credentials before saving
- 🚀 **Auto-Deploy** - GitHub push → Cloud Build (pinned KB release) → Cloud Run
- 📦 **MCP Tools** - 29 tools spanning trading partners, processes, components, runtimes, deployments, schedules, account management, and more
- 📚 **Boomi Docs KB** - Optional retrieval-augmented `search_boomi_docs` / `read_boomi_doc_page` tools backed by a pinned knowledge-base release
- ☁️ **Cloud Native** - Running on Google Cloud Run

---

## Quick Start

### For Users

1. **Visit the Web UI**: [https://boomi.renera.ai](https://boomi.renera.ai)
2. **Login with Google** - OAuth authentication
3. **Add Boomi Credentials**:
   - Email: Your Boomi account email
   - API Token: Your Boomi API token
   - Account ID: Your Boomi account ID
   - Profile Name: A name for this credential set (e.g., "production", "sandbox")

4. **Connect Claude Code**:
```bash
claude mcp add --transport http boomi https://boomi.renera.ai/mcp
```

5. **Authorize** - Browser opens for OAuth consent, click "Approve"

6. **Use MCP Tools**:
```
Show me my Boomi account information from the production profile
```

---

## Architecture

```
┌──────────────┐
│    User      │
│  (Browser)   │
└──────┬───────┘
       │ 1. Visit https://boomi.renera.ai
       │ 2. Google OAuth Login
       ▼
┌──────────────────────────────────────┐
│      Boomi MCP Server (Cloud Run)    │
│  ┌────────────┐    ┌──────────────┐ │
│  │  Web UI    │    │  MCP Server  │ │
│  │ (FastAPI)  │    │  (FastMCP)   │ │
│  └────────────┘    └──────────────┘ │
└───────┬──────────────────┬───────────┘
        │                  │
        │ Store            │ Retrieve
        │ Credentials      │ Credentials
        ▼                  ▼
┌──────────────────────────────────────┐
│      GCP Secret Manager              │
│  boomi-mcp-{user-id}-{profile-name}  │
└──────────────────────────────────────┘
                    │
                    │ API Calls
                    ▼
             ┌──────────────┐
             │  Boomi API   │
             └──────────────┘
```

---

## Available MCP Tools

The server exposes 29 tools. All tools require an authenticated session and a
valid `profile` parameter pointing at a stored Boomi credential set.

### Account & profile management
- `list_boomi_profiles()` — list saved credential profiles for the current user.
- `boomi_account_info(profile)` — fetch account details for the named profile.
- `set_boomi_credentials(...)` / `delete_boomi_profile(...)` — credential CRUD.
- `manage_account(...)`, `manage_account_groups(...)` — Boomi account admin.

### Build, deploy, and operate integrations
- `manage_process`, `manage_component`, `analyze_component`,
  `query_components`, `build_integration`, `get_schema_template`
- `manage_environments`, `manage_runtimes`, `manage_deployment`,
  `execute_process`, `troubleshoot_execution`, `manage_schedules`,
  `manage_listeners`, `manage_integration_packs`
- `manage_trading_partner`, `manage_connector`, `manage_shared_resources`,
  `manage_folders`, `monitor_platform`

### Escape hatches
- `invoke_boomi_api(...)` — call any Boomi REST endpoint when no dedicated tool
  exists.
- `list_capabilities()` — discoverability helper that summarizes all
  registered tools.

### Boomi Docs Knowledge Base (optional)

Registered only when the server starts with `BOOMI_DOCS_ENABLED=true` and a
populated KB at `BOOMI_DOCS_DB_PATH`:

- `search_boomi_docs(query, ...)` — semantic search across the indexed
  Boomi documentation corpus.
- `read_boomi_doc_page(page_key)` — fetch the full markdown for a specific
  documentation page.
- Resource `kb://boomi-docs/corpus` — corpus manifest (release tag, page
  count, generated-at metadata).

The KB corpus is built and released by
[`RenEra-ai/knowledge-base-builder`](https://github.com/RenEra-ai/knowledge-base-builder)
and embedded into the image at build time via
[`deploy/kb-release.env`](deploy/kb-release.env). See
[KB Release Promotion](#kb-release-promotion) below.

---

## Deployment

### Current Production Deployment

- **Hosting**: Google Cloud Run (us-central1)
- **URL**: https://boomi.renera.ai
- **CI/CD**: Automated via GitHub
- **Region**: us-central1
- **Authentication**: Google OAuth 2.0

### CI/CD Pipeline

Automatic deployment on push to `main` branch:

```
GitHub Push → Cloud Build (cloudbuild.yaml) → Docker Build (KB pin) → Artifact Registry → Cloud Run
```

The pipeline is source-controlled in [`cloudbuild.yaml`](cloudbuild.yaml) and
embeds a pinned Boomi Docs knowledge-base release into the image. The KB tag
lives in [`deploy/kb-release.env`](deploy/kb-release.env) so every corpus
version change is a visible repo edit — builds must never use a floating
`latest` KB release.

### KB Release Promotion

1. In `RenEra-ai/knowledge-base-builder`, cut a manual `workflow_dispatch`
   release (for example `kb-13`). The release must publish
   `boomi_knowledge_db.tar.gz` as an asset.
2. In this repo, bump the single line in `deploy/kb-release.env`:
   ```
   KB_RELEASE_TAG=kb-13
   ```
3. Open a PR with that change and merge to `main`.
4. The Cloud Build trigger reads `cloudbuild.yaml`, runs a `curl -fI`
   preflight against the GitHub release asset, then builds the image with
   `--build-arg KB_RELEASE_TAG=$KB_RELEASE_TAG`. A missing or empty pin
   fails the build before any Docker work happens.
5. Cloud Run is updated with `BOOMI_DOCS_ENABLED=true`,
   `BOOMI_DOCS_DB_PATH=/app/kb/boomi_knowledge_db`, and
   `BOOMI_DOCS_RELEASE_TAG=<tag>`, which causes the server to register the
   `search_boomi_docs` and `read_boomi_doc_page` tools plus the
   `kb://boomi-docs/corpus` resource at startup.

### Cloud Build Trigger Migration

The existing trigger `8623a6fa-3295-430a-b018-7c728ba941e8` was created from
an inline auto-generated config that did not pass `KB_RELEASE_TAG` and did
not set the KB runtime env vars. Point it at the source-controlled config
once:

```bash
gcloud builds triggers update github 8623a6fa-3295-430a-b018-7c728ba941e8 \
  --project=boomimcp \
  --region=global \
  --build-config=cloudbuild.yaml
```

After migration, every push to `main` runs the steps in `cloudbuild.yaml`
and a `git log -- cloudbuild.yaml deploy/kb-release.env` shows exactly which
KB version is live.

### Manual Deployment

If you need to deploy manually (skips the GitHub trigger):

```bash
# Authenticate with GCP
gcloud auth login
gcloud config set project boomimcp

# Submit the same pipeline that the trigger runs.
# REPO_NAME and COMMIT_SHA are populated by Cloud Build only for
# trigger-driven runs, so pass them explicitly via --substitutions
# when submitting from the CLI.
gcloud builds submit \
  --config=cloudbuild.yaml \
  --substitutions="REPO_NAME=boomi-mcp-server,COMMIT_SHA=$(git rev-parse HEAD)"

# Or use the trigger by pushing
git push origin main
```

---

## Configuration

### Environment Variables (Cloud Run)

#### OAuth Proxy Persistence

Required for MCP OAuth flow to survive Cloud Run instance sleep/restart.
OAuth state is stored in MongoDB Atlas with Fernet encryption:

```bash
OIDC_CLIENT_ID          # Google OAuth client ID
OIDC_CLIENT_SECRET      # Google OAuth client secret
OIDC_BASE_URL           # https://boomi.renera.ai
SESSION_SECRET          # Session signing key for web UI
MONGODB_URI             # MongoDB Atlas connection string for OAuth state
JWT_SIGNING_KEY         # Stable key for signing MCP JWT tokens
STORAGE_ENCRYPTION_KEY  # Fernet key(s) for encrypting OAuth tokens at rest.
                        # Single value or comma-separated list (newest first)
                        # to enable zero-downtime key rotation via MultiFernet.
```

#### Authentication hardening (optional)

All four ship with safe defaults; override only when debugging or
performing a key rotation. See `docs/oauth-migration-runbook.md` for the
full rollback procedure.

```bash
BOOMI_RT_GRACE_SECONDS          # default 60. Window during which a just-
                                # rotated refresh token still returns the
                                # same new tokens (defeats one-time-use
                                # replay race). Set 0 to disable.
BOOMI_RT_GRACE_MAX_SIZE         # default 512. LRU capacity for the grace cache.
BOOMI_OAUTH_DIAGNOSTICS_DISABLE # default off (diagnostics ON). Set true to
                                # silence the three OAuth diagnostic log
                                # patches. BOOMI_OAUTH_DIAGNOSTICS=false
                                # also works as a back-compat opt-out.
BOOMI_AUTH_HEAL_CORRUPT_CLIENTS # default true. When get_client raises
                                # InvalidToken/ValidationError, delete the
                                # corrupted MongoDB doc so the client can
                                # re-register cleanly.
```

#### OAuth cache hardening (optional)

Closes the remaining Google-call and cross-instance gaps. All ship
with safe defaults; see `docs/oauth-migration-runbook.md` for the
rollout and rollback procedure.

```bash
BOOMI_TOKEN_CACHE_DISABLE           # default off (cache ON). Set true to
                                    # restore the per-tool-call Google
                                    # tokeninfo/userinfo round trip.
BOOMI_TOKEN_CACHE_TTL_SECONDS       # default 300. Upper bound on per-entry
                                    # TTL; caps Google-revocation latency.
BOOMI_TOKEN_CACHE_MAX_SIZE          # default 256. LRU capacity.
BOOMI_TOKEN_CACHE_SWR               # default false. Opt-in stale-while-
                                    # revalidate against short Google outages.
BOOMI_TOKEN_CACHE_SWR_WINDOW        # default 30. Seconds before expiry at
                                    # which SWR serves stale + refreshes.

BOOMI_RT_GRACE_SHARED               # default true. Backs the refresh-token
                                    # grace cache with a MongoDB collection
                                    # (mcp-rt-grace, Fernet-encrypted) so
                                    # multi-replica deployments coalesce
                                    # rotations across instances.
BOOMI_RT_GRACE_SHARED_COLLECTION    # default mcp-rt-grace.
BOOMI_RT_GRACE_DISTRIBUTED_LOCK     # default false. Opt-in cross-instance
                                    # singleflight via mcp-rt-inflight-locks.
                                    # Enable only if logs show duplicate
                                    # orig_exchange calls within ms.
BOOMI_RT_GRACE_LOCK_TTL_SECONDS     # default 30. Auto-release safety bound.
BOOMI_RT_GRACE_LOCK_POLL_MS         # default 100. Follower poll interval.
```

#### User Credentials Storage

User Boomi API credentials are stored separately in GCP Secret Manager:

```bash
SECRETS_BACKEND         # gcp
GCP_PROJECT_ID          # boomimcp
```

- Format: `boomi-mcp-{user-id}-{profile-name}`
- Example: `boomi-mcp-glebuar-at-gmail-com-production`
- Encryption: At rest and in transit
- Access: IAM-controlled, audit logged

#### Boomi Docs Knowledge Base

Set on Cloud Run by `cloudbuild.yaml` to register the KB tools at startup:

```bash
BOOMI_DOCS_ENABLED       # true to register KB tools and resource
BOOMI_DOCS_DB_PATH       # /app/kb/boomi_knowledge_db (in-image corpus path)
BOOMI_DOCS_RELEASE_TAG   # operational marker, mirrors deploy/kb-release.env
```

When `BOOMI_DOCS_ENABLED` is unset or false, the KB module is not imported,
the `requirements-kb.txt` dependencies are not loaded, and the KB tools and
`kb://boomi-docs/corpus` resource are not registered.

---

## Local Development

### Prerequisites

- Python 3.11+
- Google Cloud SDK
- Access to GCP project with Secret Manager enabled

### Setup

```bash
# Clone repository
git clone https://github.com/RenEra-ai/boomi-mcp-server.git
cd boomi-mcp-server

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt -r requirements-cloud.txt

# Configure environment
cp .env.example .env
# Edit .env with your OAuth credentials
```

### Run Locally

```bash
# Set environment variables
export OIDC_CLIENT_ID="your-google-oauth-client-id"
export OIDC_CLIENT_SECRET="your-google-oauth-client-secret"
export OIDC_BASE_URL="http://localhost:8080"
export SESSION_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
export SECRETS_BACKEND=gcp
export GCP_PROJECT_ID=boomimcp

# Run server
python server_http.py
```

Visit http://localhost:8080 to access the web UI.

---

## Project Structure

```
boomi-mcp-server/
├── server.py                  # Core MCP server (FastMCP, all tool definitions)
├── server_http.py             # HTTP wrapper with OAuth middleware
├── src/boomi_mcp/
│   ├── auth.py                # Auth helpers
│   ├── cloud_auth.py          # OAuth provider implementations
│   ├── cloud_secrets.py       # Secret Manager backends (GCP/AWS/Azure)
│   ├── local_secrets.py       # Local filesystem secret backend
│   ├── credentials.py         # Credential storage models / validation
│   ├── sanitize.py            # Response sanitization helpers
│   ├── tools.py               # Shared tool helpers
│   ├── categories/            # Tool category groupings
│   ├── models/                # Pydantic models for SDK payloads
│   ├── utils/                 # Misc utilities
│   ├── xml_builders/          # Helpers that emit Boomi component XML
│   └── kb/                    # Boomi Docs knowledge-base (gated by BOOMI_DOCS_ENABLED)
│       ├── service.py         # Search + page retrieval over Chroma corpus
│       ├── manifest.py        # kb://boomi-docs/corpus resource
│       └── errors.py          # KB-specific exception types
├── templates/                 # Jinja2 web UI templates (credentials, login, ...)
├── static/                    # Web UI static assets
├── tests/                     # Unit + integration tests (incl. tests/kb)
├── docs/                      # Specs, plans, runbooks
├── agents/                    # Subagent configs (boomi-qa-tester, ...)
├── examples/                  # Usage examples
├── scripts/                   # Operational scripts
├── k8s/                       # Reference Kubernetes manifests
├── local_atom/                # Helpers for the local-atom dev profile
├── requirements.txt           # Core dependencies (FastMCP, ...)
├── requirements-cloud.txt     # Cloud provider SDKs
├── requirements-kb.txt        # KB dependencies (chromadb, sentence-transformers)
├── Dockerfile                 # Multi-stage Docker build (KB pin via ARG)
├── cloudbuild.yaml            # Cloud Build pipeline (pinned KB release)
├── deploy/
│   └── kb-release.env         # Pinned knowledge-base release tag
└── README.md                  # This file
```

---

## Security Features

### Authentication & Authorization
- ✅ Google OAuth 2.0 with PKCE
- ✅ OAuth consent screen (prevents confused deputy attacks)
- ✅ Session-based authentication with cryptographic signing
- ✅ Per-user credential isolation

### Data Protection
- ✅ HTTPS-only (enforced by Cloud Run)
- ✅ Credentials encrypted at rest (GCP Secret Manager)
- ✅ Credentials encrypted in transit (TLS)
- ✅ No credentials in environment variables
- ✅ Audit logging via Cloud Logging

### Access Control
- ✅ IAM-based access to secrets
- ✅ Profile limit (10 per user)
- ✅ Credential validation before storage
- ✅ Automatic session expiration

---

## Monitoring & Logs

### View Logs

```bash
# Recent logs
gcloud run services logs read boomi-mcp-server \
  --region us-central1 --limit 50 --project boomimcp

# Follow logs in real-time
gcloud run services logs tail boomi-mcp-server \
  --region us-central1 --project boomimcp

# Check service status
gcloud run services describe boomi-mcp-server \
  --region us-central1 --project boomimcp
```

### Cloud Console

- **Service**: https://console.cloud.google.com/run/detail/us-central1/boomi-mcp-server
- **Logs**: https://console.cloud.google.com/run/detail/us-central1/boomi-mcp-server/logs
- **Builds**: https://console.cloud.google.com/cloud-build/builds?project=boomimcp

---

## Troubleshooting

### Cannot Connect to MCP Server

1. Check service is running:
```bash
curl https://boomi.renera.ai/
```

2. Verify OAuth consent was completed:
   - Browser should open during `claude mcp add`
   - Click "Approve" on consent screen
   - Check for success message

3. Check Claude Code MCP configuration:
```bash
claude mcp list
```

### Credentials Not Saving

1. Verify all fields are filled in web UI
2. Check credential validation passes
3. Review browser console for errors (F12)
4. Check server logs for error messages

### API Errors

1. Verify Boomi credentials are correct:
   - Email should be registered in Boomi
   - API token should be valid
   - Account ID should match your account

2. Test credentials directly:
```bash
curl -u "BOOMI_TOKEN.email@example.com:your-token" \
  "https://api.boomi.com/api/rest/v1/YOUR_ACCOUNT_ID/Account/YOUR_ACCOUNT_ID"
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## Technical Details

### FastMCP Version

Currently pinned to **FastMCP 3.1.1** in `requirements.txt`. Includes:
- OAuth consent screen
- Session middleware support
- Google OAuth provider
- Server branding (custom icons, site URL)

### Session Management

- Uses `SessionMiddleware` from Starlette
- Session secret stored in GCP Secret Manager
- Sessions persist across requests via cryptographically signed cookies
- Max age: 1 hour (configurable)

### Profile Management

- Maximum 10 profiles per user
- Profile names are required (no default profile)
- Profile names must be unique per user
- Examples: "production", "sandbox", "dev", "staging"

---

## Resources

- **Live Service**: https://boomi.renera.ai
- **GitHub Repository**: https://github.com/RenEra-ai/boomi-mcp-server
- **FastMCP Documentation**: https://gofastmcp.com
- **Boomi Python SDK**: https://github.com/RenEra-ai/boomi-python
- **MCP Specification**: https://modelcontextprotocol.io
- **Boomi Platform API**: https://help.boomi.com/docs/atomsphere/integration/platform_management/c-atm-platform_api_2cf25c18-ca93-43d2-a53e-048017d0b102/

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Support

For issues, questions, or feature requests:
- Open an issue on [GitHub](https://github.com/RenEra-ai/boomi-mcp-server/issues)
- Include relevant logs (with secrets redacted)
- Describe your environment and steps to reproduce

---

## Acknowledgments

- Built with [FastMCP](https://gofastmcp.com) framework
- Integrates [Boomi Python SDK](https://github.com/RenEra-ai/boomi-python)
- Implements the [Model Context Protocol](https://modelcontextprotocol.io)
- Powered by Google Cloud Platform

---

**Last Updated**: 2026-05-18
**Status**: ✅ Production (Stable)
**Version**: FastMCP 3.1.1
