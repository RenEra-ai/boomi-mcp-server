#!/bin/bash
# Setup local development environment for Boomi MCP Server
# This creates a virtual environment and installs all dependencies

cd "$(dirname "$0")"

echo "=========================================="
echo "🔧 Setting up Local Dev Environment"
echo "=========================================="
echo ""

# Find Python 3.10+ (required by fastmcp)
PYTHON=""
for cmd in python3.13 python3.12 python3.11 python3.10; do
    if command -v $cmd &> /dev/null; then
        version=$($cmd --version 2>&1 | awk '{print $2}')
        echo "Found $cmd (version $version)"
        PYTHON=$cmd
        break
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERROR: Python 3.10+ is required for fastmcp"
    echo "Please install Python 3.10 or higher"
    exit 1
fi

echo "Using: $PYTHON"
echo ""

# Remove old venv if it exists (might have wrong paths)
if [ -d ".venv" ]; then
    echo "Removing old virtual environment..."
    rm -rf .venv
fi

# Create new virtual environment
echo "Creating virtual environment with $PYTHON..."
$PYTHON -m venv .venv

if [ ! -d ".venv" ]; then
    echo "ERROR: Failed to create virtual environment"
    exit 1
fi

# Install dependencies
echo ""
echo "Installing dependencies..."
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# Check if boomi-python is available and install it
echo ""
if [ -d "../boomi-python" ]; then
    echo "Installing boomi-python from local directory..."
    .venv/bin/pip install -e ../boomi-python
    echo "✅ boomi-python installed"
else
    echo "⚠️  WARNING: boomi-python not found at ../boomi-python"
    echo "   The server will not work without it"
    echo "   Clone it with: git clone https://github.com/RenEra-ai/boomi-python.git ../boomi-python"
    echo "   Then run this setup script again"
fi

echo ""
echo "=========================================="
echo "✅ Setup complete!"
echo "=========================================="
echo ""
echo "To start the local server, run:"
echo "  ./run_local.sh"
echo ""
