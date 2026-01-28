#!/bin/bash
# Run the local Boomi MCP server for fast development testing
# No Docker, no OAuth, just stdio MCP server

cd "$(dirname "$0")"

echo "=========================================="
echo "🚀 Starting Boomi MCP Server (Local Dev)"
echo "=========================================="
echo ""
echo "This is the LOCAL DEVELOPMENT version"
echo "No OAuth, no Docker, fast iteration"
echo ""
echo "Credentials stored in: ~/.boomi_mcp_local_secrets.json"
echo ""
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "ERROR: Virtual environment not found"
    echo "Please run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

# Use virtual environment's Python
.venv/bin/python server_local.py
