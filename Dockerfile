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

# KB corpus + model cache: fetched only when a release tag is provided at build
# time. With KB_RELEASE_TAG empty these are no-ops and the image is unchanged;
# runtime must then keep BOOMI_DOCS_ENABLED=false or startup fails fast.
RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        curl -fsSL -o /tmp/kb.tgz \
          "https://github.com/RenEra-ai/knowledge-base-builder/releases/download/${KB_RELEASE_TAG}/boomi_knowledge_db.tar.gz" && \
        tar -xzf /tmp/kb.tgz -C /app/kb && \
        rm /tmp/kb.tgz; \
    fi
RUN if [ -n "$KB_RELEASE_TAG" ]; then \
        python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"; \
    fi

# Environment variables (can be overridden at runtime)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MCP_HOST=0.0.0.0 \
    PORT=8080 \
    MCP_PATH=/mcp \
    LOG_LEVEL=info

# Health check - MCP server responds on /mcp path
# Cloud Run sets PORT dynamically, default to 8080 for health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/mcp || exit 1

# Expose port (Cloud Run will override with PORT env var)
EXPOSE 8080

# Run the MCP server directly (simpler and more reliable)
# Updated: 2025-10-15 - using server.py with native HTTP transport
CMD ["python", "server_http.py"]
