# Multi-stage Dockerfile for Boomi MCP Cloud Server
# Optimized for production deployment with minimal image size

# Stage 1: Builder
FROM python:3.11-slim AS builder

# Optional KB feature: empty by default. When unset, the KB steps below are
# no-ops and the image is materially identical to a non-KB build.
ARG KB_RELEASE_TAG=""

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files
COPY requirements.txt requirements-cloud.txt requirements-kb.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements-cloud.txt

# KB dependencies (chromadb, sentence-transformers): installed only for
# KB-enabled images so the default build stays light.
RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        pip install --no-cache-dir -r requirements-kb.txt; \
    fi

# Stage 2: Runtime
FROM python:3.11-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 -s /bin/bash appuser

# Set working directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=appuser:appuser . .

# Create data directory for SQLite database
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data

# Boomi Docs knowledge base mount point (populated only for KB-enabled builds)
ARG KB_RELEASE_TAG=""
RUN mkdir -p /app/kb && chown -R appuser:appuser /app/kb

# Switch to non-root user
USER appuser

# Build-time guard: fail the build if any high-value tool module is missing from the
# build context. Regression guard for the .gcloudignore/.gitignore upload-exclusion bug
# that silently shipped images without analyze_component and disabled 4 tool categories
# (manage_trading_partner, analyze_component, manage_connector, build_integration).
# Imports each required module explicitly so it catches any of them going missing,
# independent of the components package __init__. Lightweight (boomi SDK + stdlib, no
# torch/chromadb) and unconditional, so it runs on every build and fails in seconds —
# before the expensive KB steps below.
RUN PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app/src python -c "import boomi_mcp.categories.components.analyze_component; import boomi_mcp.categories.components.trading_partners; import boomi_mcp.categories.components.connectors; import boomi_mcp.categories.integration_builder" \
 || (echo '[BUILD ERROR] Required tool module failed to import - likely dropped from the build context by a .gcloudignore/.gitignore upload exclusion. Verify src/boomi_mcp/** is present in the uploaded build context.' >&2; exit 1)

# KB corpus + model cache: fetched only when a release tag is provided at build
# time. With KB_RELEASE_TAG empty these are no-ops and the image is unchanged;
# runtime must then keep BOOMI_DOCS_ENABLED=false or startup fails fast.
RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        curl -fsSL -o /tmp/kb.tgz \
          "https://github.com/RenEra-ai/knowledge-base-builder/releases/download/${KB_RELEASE_TAG}/boomi_knowledge_db.tar.gz" && \
        tar -xzf /tmp/kb.tgz -C /app/kb && \
        rm /tmp/kb.tgz; \
    fi
# Embedding-model cache location. Set BEFORE the preload so the model is baked
# under an explicit, appuser-owned path (no mkdir/chown needed — /home/appuser is
# already owned by appuser) that both the build-time validation below and the
# runtime warmup resolve under HF_HUB_OFFLINE. NOT the offline flags yet: the
# preload must reach Hugging Face to download the model.
ENV HF_HOME=/home/appuser/.cache/huggingface \
    SENTENCE_TRANSFORMERS_HOME=/home/appuser/.cache/sentence_transformers

# Preload resolves the model identity (name + pinned revision) from the
# DOWNLOADED corpus manifest via the same fail-closed resolver the runtime
# warmup uses — the Dockerfile no longer duplicates any model logic.
RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        PYTHONPATH=/app/src python scripts/preload_kb_model.py --db-path /app/kb/boomi_knowledge_db; \
    fi

# Build-time KB validation gate (Workstream C): run the full build_kb_service()
# against the baked corpus with the offline flags set INLINE on this RUN. This
# proves the corpus opens AND the embedding model resolves from the baked cache
# with NO network — so a corrupt corpus / chunk-count mismatch / model-cache miss
# fails the BUILD here instead of degrading to a runtime kb_unavailable. Backfills
# the fast-fail guarantee the deferred warmup removed from the import path. Gated
# by KB_RELEASE_TAG so a non-KB build skips it.
RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
        BOOMI_DOCS_ENABLED=true \
        BOOMI_DOCS_DB_PATH=/app/kb/boomi_knowledge_db \
        PYTHONPATH=/app/src \
        python -c "from boomi_mcp.kb.service import build_kb_service; build_kb_service()"; \
    fi

# Environment variables (can be overridden at runtime)
# HF_HUB_OFFLINE / TRANSFORMERS_OFFLINE: the model is baked above (build-time
# validation confirmed a cache hit), so the runtime warmup must NEVER reach the
# network — an offline lookup is a guaranteed cache hit, while a network fetch on
# a no-egress cold instance would make warming_up permanent.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MCP_HOST=0.0.0.0 \
    PORT=8080 \
    MCP_PATH=/mcp \
    LOG_LEVEL=info \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

# Health check - mode-agnostic TCP/port check (NOT GET /mcp, which would 405
# under the future stateless transport rollout and needs no new endpoint).
# Cloud Run uses its own TCP startup probe regardless; this covers other
# runtimes that honor HEALTHCHECK.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket,os,sys; s=socket.socket(); s.settimeout(5); sys.exit(0 if s.connect_ex(('127.0.0.1', int(os.getenv('PORT','8080'))))==0 else 1)"

# Expose port (Cloud Run will override with PORT env var)
EXPOSE 8080

# Run the MCP server directly (simpler and more reliable)
# Updated: 2025-10-15 - using server.py with native HTTP transport
CMD ["python", "server_http.py"]
