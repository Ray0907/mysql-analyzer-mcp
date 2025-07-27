#!/bin/bash

echo "🏗️  Building MySQL Analyzer MCP Server"
echo "🏷️  Conventions: Tables=CamelCase, Columns=snake_case"
echo "📖  Following Model Context Protocol patterns"
echo "=========================================="

# Check if Python 3.10+ is available
python_version=$(python3 --version 2>/dev/null | cut -d' ' -f2 | cut -d'.' -f1,2)
if [[ ! "$python_version" =~ ^3\.(1[0-9]|[2-9][0-9])$ ]]; then
    echo "❌ Error: Python 3.10+ is required for MCP. Found: $python_version"
    exit 1
fi

echo "✅ Python version check passed: $python_version"

# Check if uv is installed (recommended for MCP)
if command -v uv &> /dev/null; then
    echo "✅ uv is available"
    USE_UV=true
else
    echo "⚠️  uv not found, using pip instead"
    echo "💡 Consider installing uv for faster builds: curl -LsSf https://astral.sh/uv/install.sh | sh"
    USE_UV=false
fi

# Create virtual environment
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    if [ "$USE_UV" = true ]; then
        uv venv
    else
        python3 -m venv .venv
    fi
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source .venv/bin/activate

# Install/upgrade dependencies
echo "📚 Installing dependencies..."
if [ "$USE_UV" = true ]; then
    uv pip install -e .
    uv pip install -e ".[dev]"
else
    pip install --upgrade pip setuptools wheel
    pip install -e .
    pip install -e ".[dev]"
fi

# Install MCP inspector for testing (optional)
echo "🔍 Installing MCP inspector for testing..."
if command -v npm &> /dev/null; then
    npm install -g @modelcontextprotocol/inspector
    echo "✅ MCP inspector installed globally"
else
    echo "⚠️  npm not found, skipping MCP inspector installation"
    echo "💡 Install Node.js to get the MCP inspector tool"
fi

# Run tests if they exist
if [ -d "tests" ]; then
    echo "🧪 Running tests..."
    python -m pytest tests/ -v
fi

# Check code formatting
echo "🎨 Checking code formatting..."
black --check src/ || echo "⚠️  Code formatting issues found. Run 'black src/' to fix."

# Run linting
echo "🔍 Running linting..."
flake8 src/ || echo "⚠️  Linting issues found."

# Run linting
echo "🔍 Running linting..."
flake8 src/ || echo "⚠️  Linting issues found."

echo ""
echo "🎉 Build completed successfully!"
echo ""
echo "📦 Package built with MCP support"
echo "🏷️  Naming conventions: Tables=CamelCase, Columns=snake_case"
echo ""
echo "🚀 To run the MCP server:"
echo "   source .venv/bin/activate"
echo "   python src/mysql_analyzer_mcp/server.py"
echo ""
echo "🔍 To test with MCP inspector:"
echo "   npx @modelcontextprotocol/inspector python src/mysql_analyzer_mcp/server.py"
echo ""
echo "📖 To add to Claude Desktop, add this to claude_desktop_config.json:"
echo "   {"
echo "     \"mcpServers\": {"
echo "       \"mysql-analyzer\": {"
echo "         \"command\": \"$(pwd)/.venv/bin/python\","
echo "         \"args\": [\"$(pwd)/src/mysql_analyzer_mcp/server.py\"]"
echo "       }"
echo "     }"
echo "   }"
