#!/usr/bin/env python3
"""
MySQL Analyzer MCP Server

A Model Context Protocol (MCP) server for comprehensive MySQL database analysis with:
- CamelCase table naming conventions
- snake_case column naming conventions  
- Comprehensive analysis and automatic fixes

Following the official MCP Python SDK patterns from https://modelcontextprotocol.io/quickstart/server
"""

import asyncio
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# MCP SDK imports
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio as stdio
from mcp.server.models import InitializationOptions

# Local imports
try:
    from config import get_config
    import db_connector
    from analyzers import (
        index_analyzer,
        naming_analyzer,
        performance_analyzer,
        schema_analyzer,
    )
    import patch_generator
except ImportError as e:
    # Add a more helpful error message for debugging
    logging.basicConfig(level=logging.DEBUG)
    logging.error(
        f"Failed to import local modules: {e}\n"
        f"Python Path: {sys.path}\n"
        f"Current Directory: {Path.cwd()}"
    )
    raise

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("mysql-analyzer-mcp")

# Create the server instance following MCP patterns
server = Server("mysql-analyzer-mcp")

# Helper functions
async def get_database_connection(arguments: Dict[str, Any]) -> tuple:
    """
    Get database connection using provided arguments or configuration.
    Overrides default config with any provided arguments.
    """
    config_manager = get_config()
    
    # Override config with arguments if provided
    db_config = config_manager.override_db_config(**arguments)

    if not db_config.is_valid():
        raise ValueError("Database configuration is incomplete. Please provide credentials or set up your .env file.")

    try:
        connection = db_connector.get_db_connection()
        if not connection or not connection.is_connected():
            raise ConnectionError("Failed to establish database connection.")
        return connection, db_config.database
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def save_patch_file(content: str, filename: str, workspace_dir: Optional[str] = None) -> str:
    """Save SQL patch content to file."""
    if workspace_dir:
        patch_dir = Path(workspace_dir) / 'patches'
    else:
        patch_dir = Path('patches')
    
    patch_dir.mkdir(exist_ok=True)
    filepath = patch_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info(f"Saved patch file: {filepath}")
    return str(filepath)

# MCP Tools - following official @mcp.tool() decorator pattern

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools for the MySQL analyzer."""
    return [
        types.Tool(
            name="analyze_naming_conventions",
            description="Analyze and enforce MySQL naming conventions (CamelCase tables, snake_case columns)",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_host": {
                        "type": "string",
                        "description": "Database host (optional, defaults to .env)"
                    },
                    "db_user": {
                        "type": "string", 
                        "description": "Database username (optional, defaults to .env)"
                    },
                    "db_password": {
                        "type": "string",
                        "description": "Database password (optional, defaults to .env)"
                    },
                    "db_database": {
                        "type": "string",
                        "description": "Database name (optional, defaults to .env)"
                    },
                    "fix_issues": {
                        "type": "boolean",
                        "description": "Generate SQL fixes automatically",
                        "default": True
                    },
                    "workspace_dir": {
                        "type": "string",
                        "description": "Directory to save SQL patch files (optional)"
                    }
                },
                "required": []
            }
        ),
        types.Tool(
            name="analyze_database_indexes", 
            description="Analyze database indexes for naming conventions and redundancy issues",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_host": {"type": "string", "description": "Database host (optional)"},
                    "db_user": {"type": "string", "description": "Database username (optional)"},
                    "db_password": {"type": "string", "description": "Database password (optional)"},
                    "db_database": {"type": "string", "description": "Database name (optional)"}
                },
                "required": []
            }
        ),
        types.Tool(
            name="analyze_database_performance",
            description="Analyze database performance issues and bottlenecks",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_host": {"type": "string", "description": "Database host (optional)"},
                    "db_user": {"type": "string", "description": "Database username (optional)"}, 
                    "db_password": {"type": "string", "description": "Database password (optional)"},
                    "db_database": {"type": "string", "description": "Database name (optional)"}
                },
                "required": []
            }
        ),
        types.Tool(
            name="analyze_database_schema",
            description="Analyze database schema compliance with MySQL best practices",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_host": {"type": "string", "description": "Database host (optional)"},
                    "db_user": {"type": "string", "description": "Database username (optional)"},
                    "db_password": {"type": "string", "description": "Database password (optional)"},
                    "db_database": {"type": "string", "description": "Database name (optional)"}
                },
                "required": []
            }
        ),
        types.Tool(
            name="comprehensive_analysis",
            description="Run all analysis types and generate comprehensive report with SQL patches",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_host": {"type": "string", "description": "Database host (optional)"},
                    "db_user": {"type": "string", "description": "Database username (optional)"},
                    "db_password": {"type": "string", "description": "Database password (optional)"},
                    "db_database": {"type": "string", "description": "Database name (optional)"},
                    "generate_patches": {"type": "boolean", "description": "Generate SQL patches", "default": True},
                    "workspace_dir": {"type": "string", "description": "Directory to save files (optional)"}
                },
                "required": []
            }
        ),
        types.Tool(
            name="generate_sql_patches",
            description="Generate SQL patch files to fix identified issues",
            inputSchema={
                "type": "object",
                "properties": {
                    "db_host": {"type": "string", "description": "Database host (optional)"},
                    "db_user": {"type": "string", "description": "Database username (optional)"},
                    "db_password": {"type": "string", "description": "Database password (optional)"},
                    "db_database": {"type": "string", "description": "Database name (optional)"},
                    "patch_type": {
                        "type": "string",
                        "enum": ["naming", "indexes", "performance", "schema", "comprehensive"],
                        "description": "Type of patches to generate",
                        "default": "comprehensive"
                    },
                    "workspace_dir": {"type": "string", "description": "Directory to save patch files (optional)"}
                },
                "required": []
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """Handle tool calls following MCP patterns."""
    
    logger.info(f"Executing tool: {name}")
    
    try:
        if name == "analyze_naming_conventions":
            result = await tool_analyze_naming_conventions(arguments)
        elif name == "analyze_database_indexes":
            result = await tool_analyze_database_indexes(arguments)
        elif name == "analyze_database_performance":
            result = await tool_analyze_database_performance(arguments)
        elif name == "analyze_database_schema":
            result = await tool_analyze_database_schema(arguments)
        elif name == "comprehensive_analysis":
            result = await tool_comprehensive_analysis(arguments)
        elif name == "generate_sql_patches":
            result = await tool_generate_sql_patches(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
        
        return [types.TextContent(type="text", text=result)]
        
    except Exception as e:
        logger.error(f"Tool execution error for {name}: {e}")
        import traceback
        error_msg = f"❌ Error executing {name}: {str(e)}\n\nDebug info:\n{traceback.format_exc()}"
        return [types.TextContent(type="text", text=error_msg)]

# Tool implementations

async def tool_analyze_naming_conventions(arguments: Dict[str, Any]) -> str:
    """Analyze naming conventions tool (CamelCase tables, snake_case columns)."""
    
    connection, db_name = await get_database_connection(arguments)
    cursor = None
    
    try:
        cursor = connection.cursor()
        logger.info(f"Starting naming analysis for database: {db_name}")
        
        # Run naming analysis
        analysis_result = naming_analyzer.run_naming_analysis(cursor, db_name)
        
        # Format the report
        report = naming_analyzer.format_naming_report(analysis_result)
        
        # Generate SQL fixes if requested
        fix_issues = arguments.get("fix_issues", True)
        workspace_dir = arguments.get("workspace_dir")
        
        if fix_issues and analysis_result.get('issues'):
            sql_fixes = naming_analyzer.generate_naming_fix_sql(cursor, analysis_result['issues'], db_name)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"naming_fixes_{db_name}_{timestamp}.sql"
            
            filepath = save_patch_file('\n'.join(sql_fixes), filename, workspace_dir)
            
            report += f"\n\n## 🔧 SQL Fixes Generated\n\n"
            report += f"✅ **Patch file created:** `{filepath}`\n\n"
            report += f"⚠️ **Important:** Review and test all patches in a development environment before applying to production!\n"
        
        logger.info(f"Naming analysis completed")
        return report
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

async def tool_analyze_database_indexes(arguments: Dict[str, Any]) -> str:
    """Analyze database indexes."""
    
    connection, db_name = await get_database_connection(arguments)
    cursor = None
    
    try:
        cursor = connection.cursor()
        logger.info(f"Starting index analysis for database: {db_name}")
        
        index_report = index_analyzer.run_index_analysis(cursor, db_name)
        
        if not index_report:
            return "✅ **Index Analysis Complete**\n\nNo index issues found! All indexes follow proper naming conventions and no redundant indexes detected."
        
        result = ["📊 **MySQL Index Analysis Report**", "=" * 40, ""]
        
        for table, issues in index_report.items():
            result.append(f"### Table: `{table}`")
            
            # Group issues by type for better readability
            naming_issues = [i for i in issues if i['type'] == 'RENAME_INDEX']
            redundancy_issues = [i for i in issues if i['type'] == 'DROP_INDEX']
            performance_issues = [i for i in issues if i['type'] == 'LOW_CARDINALITY_INDEX']

            if naming_issues:
                result.append("\n**🏷️ Index Naming Issues:**")
                for issue in naming_issues:
                    result.append(f"- {issue['description']}")
            
            if redundancy_issues:
                result.append("\n**🔄 Redundant Indexes:**")
                for issue in redundancy_issues:
                    result.append(f"- {issue['description']}")
            
            if performance_issues:
                result.append("\n**⚡ Performance Issues:**")
                for issue in performance_issues:
                    result.append(f"- {issue['description']}")
            
            result.append("")
        
        return "\n".join(result)
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

async def tool_analyze_database_performance(arguments: Dict[str, Any]) -> str:
    """Analyze database performance."""
    
    connection, db_name = await get_database_connection(arguments)
    cursor = None
    
    try:
        cursor = connection.cursor()
        logger.info(f"Starting performance analysis for database: {db_name}")
        
        performance_report = performance_analyzer.analyze_performance(cursor, db_name)
        
        if not performance_report:
            return "✅ **Performance Analysis Complete**\n\nNo performance issues detected! Database appears to be well-optimized."
        
        result = ["⚡ **MySQL Performance Analysis Report**", "=" * 40, ""]
        
        for table, issues in performance_report.items():
            result.append(f"### Table: `{table}`")
            
            for issue in issues:
                severity_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(issue.get('severity', 'low'), "ℹ️")
                result.append(f"{severity_emoji} **{issue.get('severity', 'low').upper()}**: {issue.get('description')}")
            
            result.append("")
        
        return "\n".join(result)
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

async def tool_analyze_database_schema(arguments: Dict[str, Any]) -> str:
    """Analyze database schema."""
    
    connection, db_name = await get_database_connection(arguments)
    cursor = None
    
    try:
        cursor = connection.cursor()
        logger.info(f"Starting schema analysis for database: {db_name}")
        
        schema_report = schema_analyzer.analyze_schema(cursor, db_name)
        
        if not schema_report:
            return "✅ **Schema Analysis Complete**\n\nSchema is compliant! All tables use recommended settings."
        
        result = ["🏗️ **MySQL Schema Analysis Report**", "=" * 40, ""]
        
        for table, issues in schema_report.items():
            result.append(f"### Table: `{table}`")
            
            for issue in issues:
                severity_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(issue.get('severity', 'low'), "ℹ️")
                result.append(f"{severity_emoji} **{issue.get('severity', 'low').upper()}**: {issue.get('description')}")
            
            result.append("")
        
        return "\n".join(result)
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

async def tool_comprehensive_analysis(arguments: Dict[str, Any]) -> str:
    """Run comprehensive analysis."""
    
    connection, db_name = await get_database_connection(arguments)
    cursor = None
    
    try:
        cursor = connection.cursor()
        logger.info(f"Starting comprehensive analysis for database: {db_name}")
        
        # Run all analyses
        naming_analysis = naming_analyzer.run_naming_analysis(cursor, db_name)
        index_report = index_analyzer.run_index_analysis(cursor, db_name)
        performance_report = performance_analyzer.analyze_performance(cursor, db_name)
        schema_report = schema_analyzer.analyze_schema(cursor, db_name)
        
        # Count issues by severity
        critical_count = medium_count = low_count = 0
        
        # Count naming issues
        if naming_analysis.get('issues'):
            for table, issues in naming_analysis['issues'].items():
                for issue in issues:
                    if issue.get('severity') == 'critical':
                        critical_count += 1
                    elif issue.get('severity') in ['medium', 'high']:
                        medium_count += 1
                    elif issue.get('severity') == 'low':
                        low_count += 1
        
        # Count from other analyses
        for report in [performance_report, schema_report]:
            for table, issues in report.items():
                for issue in issues:
                    if issue.get('severity') == 'critical':
                        critical_count += 1
                    elif issue.get('severity') in ['medium', 'high']:
                        medium_count += 1
                    elif issue.get('severity') == 'low':
                        low_count += 1
        
        # Generate report
        result = [
            "🔍 **MySQL Comprehensive Analysis Report**",
            "=" * 50,
            f"**Database:** `{db_name}`",
            "**Conventions:** Tables=CamelCase, Columns=snake_case",
            f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 📋 Executive Summary",
            f"- 🔴 Critical Issues: {critical_count}",
            f"- 🟡 Medium/High Issues: {medium_count}",
            f"- 🟢 Low Priority Issues: {low_count}",
            ""
        ]
        
        # Add detailed analysis sections
        if naming_analysis.get('issues'):
            result.extend([
                "## 🏷️ Naming Conventions Analysis",
                naming_analyzer.format_naming_report(naming_analysis),
                ""
            ])
        
        if index_report:
            result.append("## 📊 Index Analysis")
            for table, issues in index_report.items():
                result.append(f"### Table: `{table}`")
                
                # Group issues by type for better readability
                naming_issues = [i for i in issues if i['type'] == 'RENAME_INDEX']
                redundancy_issues = [i for i in issues if i['type'] == 'DROP_INDEX']
                performance_issues = [i for i in issues if i['type'] == 'LOW_CARDINALITY_INDEX']

                if naming_issues:
                    result.append("\n**🏷️ Index Naming Issues:**")
                    for issue in naming_issues:
                        result.append(f"- {issue['description']}")
                
                if redundancy_issues:
                    result.append("\n**🔄 Redundant Indexes:**")
                    for issue in redundancy_issues:
                        result.append(f"- {issue['description']}")
                
                if performance_issues:
                    result.append("\n**⚡ Performance Issues:**")
                    for issue in performance_issues:
                        result.append(f"- {issue['description']}")
                
                result.append("")
        
        # Generate patches if requested
        generate_patches = arguments.get("generate_patches", True)
        workspace_dir = arguments.get("workspace_dir")
        
        if generate_patches and any([naming_analysis.get('issues'), index_report, performance_report, schema_report]):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Generate naming patches
            if naming_analysis.get('issues'):
                naming_fixes = naming_analyzer.generate_naming_fix_sql(cursor, naming_analysis['issues'], db_name)
                naming_file = f"naming_fixes_{db_name}_{timestamp}.sql"
                naming_path = save_patch_file('\n'.join(naming_fixes), naming_file, workspace_dir)
                
                result.extend([
                    "## 🔧 SQL Patches Generated",
                    f"✅ **Naming fixes:** `{naming_path}`",
                    "",
                    "⚠️ **Important:** Review and test all patches in a development environment before applying to production!",
                    ""
                ])
        
        if not any([naming_analysis.get('issues'), index_report, performance_report, schema_report]):
            result.extend([
                "## ✅ All Clear!",
                "No issues found in any analysis category. Your database follows all naming conventions and best practices.",
                ""
            ])
        
        return "\n".join(result)
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

async def tool_generate_sql_patches(arguments: Dict[str, Any]) -> str:
    """Generate SQL patches."""
    
    connection, db_name = await get_database_connection(arguments)
    cursor = None
    
    try:
        cursor = connection.cursor()
        patch_type = arguments.get("patch_type", "comprehensive")
        workspace_dir = arguments.get("workspace_dir")
        
        logger.info(f"Generating {patch_type} patches for database: {db_name}")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if patch_type == "naming":
            naming_analysis = naming_analyzer.run_naming_analysis(cursor, db_name)
            if not naming_analysis.get('issues'):
                return "✅ No naming issues found - no patches needed!"
            
            sql_fixes = naming_analyzer.generate_naming_fix_sql(cursor, naming_analysis['issues'], db_name)
            filename = f"naming_fixes_{db_name}_{timestamp}.sql"
            filepath = save_patch_file('\n'.join(sql_fixes), filename, workspace_dir)
            
        elif patch_type == "comprehensive":
            # Run all analyses
            naming_analysis = naming_analyzer.run_naming_analysis(cursor, db_name)
            index_report = index_analyzer.run_index_analysis(cursor, db_name)
            performance_report = performance_analyzer.analyze_performance(cursor, db_name)
            schema_report = schema_analyzer.analyze_schema(cursor, db_name)
            
            if not any([naming_analysis.get('issues'), index_report, performance_report, schema_report]):
                return "✅ No issues found - no patches needed!"
            
            # Generate comprehensive patches
            all_patches = []
            
            if naming_analysis.get('issues'):
                naming_fixes = naming_analyzer.generate_naming_fix_sql(cursor, naming_analysis['issues'], db_name)
                all_patches.extend(naming_fixes)
            
            filename = f"comprehensive_fixes_{db_name}_{timestamp}.sql"
            filepath = save_patch_file('\n'.join(all_patches), filename, workspace_dir)
        
        else:
            raise ValueError(f"Unknown patch type: {patch_type}")
        
        return f"""🔧 **SQL Patches Generated Successfully**

**Patch Type:** {patch_type.title()}
**Database:** `{db_name}`
**File:** `{filepath}`

⚠️ **Before applying these patches:**
1. **Backup your database**
2. **Review each SQL statement carefully** 
3. **Test in development environment first**
4. **Apply during maintenance window**
5. **Update application code for naming changes**

The patch file contains detailed comments explaining each change."""

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# Main function following MCP patterns
async def main():
    """Main entry point for the MCP server."""
    logger.info("🚀 Starting MySQL Analyzer MCP Server")
    logger.info("🏷️ Naming Conventions: Tables=CamelCase, Columns=snake_case")
    logger.info("📖 Following Model Context Protocol patterns")
    
    # Use stdin/stdout streams for MCP communication
    async with stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mysql-analyzer-mcp",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

# Entry point
if __name__ == "__main__":
    asyncio.run(main())
