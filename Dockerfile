# Multi-stage Dockerfile for Boomi MCP Cloud Server
# Optimized for production deployment with minimal image size

# Stage 1: Builder
FROM python:3.11-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files and local FastMCP
COPY requirements.txt requirements-cloud.txt ./
COPY fastmcp ./fastmcp

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements-cloud.txt

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

# Switch to non-root user
USER appuser

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
