# MySQL Analyzer MCP Server

A standalone Model Context Protocol (MCP) server providing a suite of tools for static analysis of MySQL databases, focusing on schema integrity, naming conventions, and performance metrics.

## 🏷️ Naming Conventions

This MCP server enforces modern, consistent naming conventions:

- **Tables**: `CamelCase` (e.g., `Users`, `ProductCategories`)
- **Columns**: `snake_case` (e.g., `user_id`, `created_at`)
- **Indexes & Constraints**: `snake_case` with prefixes (e.g., `uk_users_email`, `idx_orders_status`)

## 🚀 Quick Start

### 1. Setup Environment

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Then, edit the `.env` file with your database credentials.

### 2. Build and Install

Run the build script to set up the virtual environment and install dependencies:

```bash
./build.sh
```

### 3. Run the MCP Server

Activate the virtual environment and run the server:

```bash
source .venv/bin/activate
python src/mysql_analyzer_mcp/server.py
```

### 4. Test with MCP Inspector

You can test the server using the MCP inspector:

```bash
npx @modelcontextprotocol/inspector python src/mysql_analyzer_mcp/server.py
```

## 🛠️ MCP Tools

This server provides a suite of tools to analyze and improve your MySQL database.

### Available Tools

- `analyze_naming_conventions`: Analyzes table and column names and can generate SQL patches to fix them.
- `analyze_database_indexes`: Checks for redundant or missing indexes.
- `analyze_database_performance`: Identifies potential performance bottlenecks.
- `analyze_database_schema`: Validates schema best practices (e.g., charset, collation).
- `comprehensive_analysis`: Runs all analyses and provides a summary report.
- `generate_sql_patches`: Generates SQL patches for specific issues.

### Example Usage in Claude

- "Analyze my MySQL database naming conventions and generate fixes."
- "Run a comprehensive analysis of my database."
- "Check my database indexes for redundancy."

## 🏗️ Project Structure

```bash
mysql-analyzer-mcp/
├── src/
│   └── mysql_analyzer_mcp/
│       ├── __init__.py
│       └── server.py              # Main MCP server logic using the official MCP SDK
├── analyzers/
│   ├── __init__.py
│   ├── naming_analyzer.py
│   ├── index_analyzer.py
│   ├── schema_analyzer.py
│   ├── performance_analyzer.py
│   └── utils.py                 # Shared database utility functions
├── build.sh                       # Build and setup script
├── config.py                      # Centralized configuration management
├── db_connector.py                # Database connection handling
├── patch_generator.py             # SQL patch generation logic
├── pyproject.toml                 # Package configuration
├── requirements.txt               # Dependencies
└── .env.example                   # Environment template
```

## 🔧 Configuration

All tool arguments and environment variables are documented within the server's tool definitions and can be inspected via MCP.

---

**Built with ❤️ following Model Context Protocol standards.**
