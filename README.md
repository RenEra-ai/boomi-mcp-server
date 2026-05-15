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
- 🚀 **Auto-Deploy** - GitHub push → Cloud Build → Cloud Run
- 📦 **MCP Tools** - Account info, profile management
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

### 1. `boomi_account_info(profile: str)`

Get Boomi account information from a specific profile.

**Parameters:**
- `profile` (required): Profile name (e.g., "production", "sandbox")

**Example:**
```
Get account info from the production profile
```

### 2. `list_boomi_profiles()`

List all saved Boomi credential profiles for the authenticated user.

**Example:**
```
Show me my Boomi profiles
```

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
   release (for example `kb-5`). The release must publish
   `boomi_knowledge_db.tar.gz` as an asset.
2. In this repo, bump the single line in `deploy/kb-release.env`:
   ```
   KB_RELEASE_TAG=kb-5
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
STORAGE_ENCRYPTION_KEY  # Fernet key for encrypting OAuth tokens at rest
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
├── server.py                  # Core MCP server with FastMCP
├── server_http.py             # HTTP wrapper with OAuth middleware
├── src/boomi_mcp/
│   ├── cloud_auth.py          # OAuth provider implementations
│   ├── cloud_secrets.py       # Secret Manager backends (GCP/AWS/Azure)
│   └── tools.py               # MCP tool implementations
├── templates/
│   ├── credentials.html       # Web UI for credential management
│   └── login.html             # OAuth login page
├── static/
│   └── favicon.png            # RenEra logo
├── requirements.txt           # Core dependencies
├── requirements-cloud.txt     # Cloud provider SDKs
├── Dockerfile                 # Multi-stage Docker build
├── .env.example               # Environment configuration template
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

Currently using **FastMCP 2.13.0** which includes:
- OAuth consent screen
- Session middleware support
- Google OAuth provider

**Note**: Server branding (custom icons, site URL) requires FastMCP 2.14.0+ (not yet released).

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

**Last Updated**: 2025-10-28
**Status**: ✅ Production (Stable)
**Version**: FastMCP 2.13.0
