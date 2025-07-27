#!/usr/bin/env python3
"""
Simple MCP Server Implementation for MySQL Analyzer

This is a simplified version that works with the basic MCP protocol
without requiring complex dependencies.
"""

import asyncio
import json
import sys
import logging
from typing import Dict, Any, List

import db_connector
from analyzers import index_analyzer, performance_analyzer, schema_analyzer, naming_analyzer
import patch_generator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mysql-analyzer-mcp")


class SimpleMCPServer:
    """Simple MCP server implementation."""
    
    def __init__(self):
        self.tools = [
            {
                "name": "analyze_indexes",
                "description": "Analyze database indexes for naming conventions and redundancy issues",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "DB_HOST": {"type": "string", "description": "Database host (optional)"},
                        "DB_USER": {"type": "string", "description": "Database username (optional)"},
                        "DB_PASSWORD": {"type": "string", "description": "Database password (optional)"},
                        "DB_DATABASE": {"type": "string", "description": "Database name (optional)"}
                    }
                }
            },
            {
                "name": "analyze_naming_conventions",
                "description": "Analyze and enforce consistent naming conventions across tables, columns, and indexes",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "DB_HOST": {"type": "string", "description": "Database host (optional)"},
                        "DB_USER": {"type": "string", "description": "Database username (optional)"},
                        "DB_PASSWORD": {"type": "string", "description": "Database password (optional)"},
                        "DB_DATABASE": {"type": "string", "description": "Database name (optional)"},
                        "fix_issues": {"type": "boolean", "description": "Automatically generate SQL fixes for naming issues", "default": True}
                    }
                }
            },
            {
                "name": "analyze_performance", 
                "description": "Analyze database performance issues",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "DB_HOST": {"type": "string", "description": "Database host (optional)"},
                        "DB_USER": {"type": "string", "description": "Database username (optional)"},
                        "DB_PASSWORD": {"type": "string", "description": "Database password (optional)"},
                        "DB_DATABASE": {"type": "string", "description": "Database name (optional)"}
                    }
                }
            },
            {
                "name": "analyze_schema",
                "description": "Analyze database schema compliance",
                "inputSchema": {
                    "type": "object", 
                    "properties": {
                        "DB_HOST": {"type": "string", "description": "Database host (optional)"},
                        "DB_USER": {"type": "string", "description": "Database username (optional)"},
                        "DB_PASSWORD": {"type": "string", "description": "Database password (optional)"},
                        "DB_DATABASE": {"type": "string", "description": "Database name (optional)"}
                    }
                }
            },
            {
                "name": "comprehensive_analysis",
                "description": "Run all analysis types and generate comprehensive report",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "DB_HOST": {"type": "string", "description": "Database host (optional)"},
                        "DB_USER": {"type": "string", "description": "Database username (optional)"},
                        "DB_PASSWORD": {"type": "string", "description": "Database password (optional)"},
                        "DB_DATABASE": {"type": "string", "description": "Database name (optional)"},
                        "generate_patches": {"type": "boolean", "description": "Generate SQL patches", "default": True}
                    }
                }
            },
            {
                "name": "generate_sql_patches",
                "description": "Generate SQL patch files to fix identified issues",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "DB_HOST": {"type": "string", "description": "Database host (optional)"},
                        "DB_USER": {"type": "string", "description": "Database username (optional)"},
                        "DB_PASSWORD": {"type": "string", "description": "Database password (optional)"},
                        "DB_DATABASE": {"type": "string", "description": "Database name (optional)"},
                        "patch_type": {"type": "string", "enum": ["index", "performance", "schema", "comprehensive"], "default": "comprehensive"}
                    }
                }
            }
        ]

    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP request."""
        method = request.get("method")
        params = request.get("params", {})
        request_id = request.get("id")

        try:
            if method == "initialize":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {},
                            "prompts": {},
                            "resources": {}
                        },
                        "serverInfo": {
                            "name": "mysql-analyzer",
                            "version": "1.0.0"
                        }
                    }
                }

            elif method == "tools/list":
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "tools": self.tools
                    }
                }

            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                
                result = await self.call_tool(tool_name, arguments)
                
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": result
                            }
                        ]
                    }
                }

            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}"
                    }
                }

        except Exception as e:
            logger.error(f"Error handling request: {e}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                }
            }

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        """Execute tool and return result."""
        try:
            # Get database connection
            connection = self.get_db_connection_with_args(arguments)
            db_name = self.get_db_name_with_args(arguments)
            
            if not connection or not db_name:
                return "❌ Error: Could not establish database connection. Please check your credentials in .env file or provide them as arguments."
            
            cursor = None
            try:
                cursor = connection.cursor()
                
                if name == "analyze_indexes":
                    return await self.run_index_analysis(cursor, db_name)
                    
                elif name == "analyze_naming_conventions":
                    return await self.run_naming_analysis(cursor, db_name, arguments.get("fix_issues", True))
                    
                elif name == "analyze_performance":
                    return await self.run_performance_analysis(cursor, db_name)
                    
                elif name == "analyze_schema":
                    return await self.run_schema_analysis(cursor, db_name)
                    
                elif name == "comprehensive_analysis":
                    return await self.run_comprehensive_analysis(cursor, db_name, arguments.get("generate_patches", True))
                    
                elif name == "generate_sql_patches":
                    return await self.generate_patches(cursor, db_name, arguments.get("patch_type", "comprehensive"))
                    
                else:
                    return f"❌ Error: Unknown tool '{name}'"
                    
            finally:
                if cursor:
                    cursor.close()
                if connection and connection.is_connected():
                    connection.close()
                    
        except Exception as e:
            logger.error(f"Error executing tool {name}: {e}")
            return f"❌ Error executing {name}: {str(e)}"

    def get_db_connection_with_args(self, arguments: Dict[str, Any]):
        """Get database connection using provided arguments or .env defaults."""
        if any(k in arguments for k in ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE']):
            # Use provided credentials
            import mysql.connector
            from mysql.connector import Error
            try:
                connection = mysql.connector.connect(
                    host=arguments.get('DB_HOST', 'localhost'),
                    user=arguments.get('DB_USER'),
                    password=arguments.get('DB_PASSWORD'),
                    database=arguments.get('DB_DATABASE')
                )
                if connection.is_connected():
                    return connection
            except Error as e:
                logger.error(f"Error connecting with provided credentials: {e}")
                return None
        else:
            # Use .env file
            return db_connector.get_db_connection()

    def get_db_name_with_args(self, arguments: Dict[str, Any]):
        """Get database name using provided arguments or .env defaults."""
        if 'DB_DATABASE' in arguments:
            return arguments['DB_DATABASE']
        else:
            return db_connector.get_db_name()

    async def run_index_analysis(self, cursor, db_name: str) -> str:
        """Run index analysis and return formatted results."""
        try:
            index_report = index_analyzer.run_index_analysis(cursor, db_name)
            
            if not index_report:
                return "✅ **Index Analysis Complete**\n\nNo index issues found! All indexes follow proper naming conventions and no redundant indexes detected."
            
            result = ["📊 **MySQL Index Analysis Report**", "=" * 40, ""]
            
            for table, issues in index_report.items():
                result.append(f"### Table: `{table}`")
                
                if issues.get('naming_issues'):
                    result.append("\n**🏷️ Index Naming Issues:**")
                    for issue in issues['naming_issues']:
                        result.append(f"- {issue}")
                
                if issues.get('redundant_indexes'):
                    result.append("\n**🔄 Redundant Indexes:**")
                    for issue in issues['redundant_indexes']:
                        result.append(f"- {issue}")
                
                if issues.get('performance_issues'):
                    result.append("\n**⚡ Performance Issues:**")
                    for issue in issues['performance_issues']:
                        result.append(f"- {issue}")
                
                result.append("")
            
            return "\n".join(result)
            
        except Exception as e:
            return f"❌ Error during index analysis: {str(e)}"
    
    async def run_naming_analysis(self, cursor, db_name: str, fix_issues: bool = True) -> str:
        """Run naming convention analysis and return formatted results."""
        try:
            analysis_result = naming_analyzer.run_naming_analysis(cursor, db_name)
            
            # Format the report
            report = naming_analyzer.format_naming_report(analysis_result)
            
            # Generate SQL fixes if requested and issues exist
            if fix_issues and analysis_result['issues']:
                sql_fixes = naming_analyzer.generate_naming_fix_sql(analysis_result['issues'], db_name)
                
                # Save to file
                import os
                patch_dir = 'patches'
                os.makedirs(patch_dir, exist_ok=True)
                
                timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"naming_fixes_{db_name}_{timestamp}.sql"
                filepath = os.path.join(patch_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(sql_fixes))
                
                report += f"\n\n## 🔧 SQL Fixes Generated\n\n"
                report += f"✅ **Patch file created:** `{filepath}`\n\n"
                report += f"⚠️ **Important:** Review and test all patches in a development environment before applying to production!\n"
            
            return report
            
        except Exception as e:
            return f"❌ Error during naming analysis: {str(e)}"

    async def run_performance_analysis(self, cursor, db_name: str) -> str:
        """Run performance analysis and return formatted results."""
        try:
            performance_report = performance_analyzer.analyze_performance(cursor, db_name)
            
            if not performance_report:
                return "✅ **Performance Analysis Complete**\n\nNo performance issues detected! Database appears to be well-optimized."
            
            result = ["⚡ **MySQL Performance Analysis Report**", "=" * 40, ""]
            
            for table, issues in performance_report.items():
                result.append(f"### Table: `{table}`")
                
                for issue in issues:
                    severity_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(issue['severity'], "ℹ️")
                    result.append(f"{severity_emoji} **{issue['severity'].upper()}**: {issue['description']}")
                
                result.append("")
            
            return "\n".join(result)
            
        except Exception as e:
            return f"❌ Error during performance analysis: {str(e)}"

    async def run_schema_analysis(self, cursor, db_name: str) -> str:
        """Run schema analysis and return formatted results."""
        try:
            schema_report = schema_analyzer.analyze_schema(cursor, db_name)
            
            if not schema_report:
                return "✅ **Schema Analysis Complete**\n\nSchema is compliant! All tables use recommended settings."
            
            result = ["🏗️ **MySQL Schema Analysis Report**", "=" * 40, ""]
            
            for table, issues in schema_report.items():
                result.append(f"### Table: `{table}`")
                
                for issue in issues:
                    severity_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}.get(issue['severity'], "ℹ️")
                    result.append(f"{severity_emoji} **{issue['severity'].upper()}**: {issue['description']}")
                
                result.append("")
            
            return "\n".join(result)
            
        except Exception as e:
            return f"❌ Error during schema analysis: {str(e)}"

    async def run_comprehensive_analysis(self, cursor, db_name: str, generate_patches: bool = True) -> str:
        """Run all analyses and return comprehensive report."""
        try:
            result = ["🔍 **MySQL Comprehensive Analysis Report**", "=" * 50, f"**Database:** `{db_name}`", ""]
            
            # Run all analyses
            index_report = index_analyzer.run_index_analysis(cursor, db_name)
            raw_index_data = index_analyzer.analyze_indexes(cursor, db_name)
            performance_report = performance_analyzer.analyze_performance(cursor, db_name)
            schema_report = schema_analyzer.analyze_schema(cursor, db_name)
            naming_analysis = naming_analyzer.run_naming_analysis(cursor, db_name)
            
            # Count issues by severity
            critical_count = medium_count = low_count = 0
            
            # Count from raw analysis data
            for report in [raw_index_data, performance_report, schema_report]:
                for table, issues in report.items():
                    if isinstance(issues, list):
                        for issue in issues:
                            if issue.get('severity') == 'critical':
                                critical_count += 1
                            elif issue.get('severity') in ['medium', 'high']:
                                medium_count += 1
                            elif issue.get('severity') == 'low':
                                low_count += 1
            
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
            
            # Summary
            all_analyzed_tables = set()
            all_analyzed_tables.update(raw_index_data.keys())
            all_analyzed_tables.update(performance_report.keys())
            all_analyzed_tables.update(schema_report.keys())
            if naming_analysis.get('issues'):
                all_analyzed_tables.update(naming_analysis['issues'].keys())
            
            result.extend([
                "## 📋 Executive Summary",
                f"- 🔴 Critical Issues: {critical_count}",
                f"- 🟡 Medium/High Issues: {medium_count}",
                f"- 🟢 Low Priority Issues: {low_count}",
                f"- 📊 Total Tables Analyzed: {len(all_analyzed_tables)}",
                ""
            ])
            
            # Add detailed analysis sections
            if naming_analysis.get('issues'):
                result.extend([
                    "## 🏷️ Naming Conventions Analysis",
                    await self.run_naming_analysis(cursor, db_name, False),  # Don't generate files in comprehensive mode
                    ""
                ])
            
            if index_report:
                result.extend([
                    "## 📊 Index Analysis",
                    await self.run_index_analysis(cursor, db_name),
                    ""
                ])
            
            if performance_report:
                result.extend([
                    "## ⚡ Performance Analysis", 
                    await self.run_performance_analysis(cursor, db_name),
                    ""
                ])
            
            if schema_report:
                result.extend([
                    "## 🏗️ Schema Analysis",
                    await self.run_schema_analysis(cursor, db_name),
                    ""
                ])
            
            # Generate patches if requested
            if generate_patches and any([raw_index_data, performance_report, schema_report, naming_analysis.get('issues')]):
                patch_file = patch_generator.generate_comprehensive_patch(
                    raw_index_data, schema_report, performance_report, db_name
                )
                
                # Also generate naming fixes if there are naming issues
                if naming_analysis.get('issues'):
                    naming_fixes = naming_analyzer.generate_naming_fix_sql(naming_analysis['issues'], db_name)
                    import os
                    patch_dir = 'patches'
                    os.makedirs(patch_dir, exist_ok=True)
                    
                    timestamp = __import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')
                    naming_patch_file = os.path.join(patch_dir, f"naming_fixes_{db_name}_{timestamp}.sql")
                    
                    with open(naming_patch_file, 'w', encoding='utf-8') as f:
                        f.write('\n'.join(naming_fixes))
                
                result.extend([
                    "## 🔧 SQL Patches Generated",
                    f"✅ **Main patch file created:** `{patch_file}`"
                ])
                
                if naming_analysis.get('issues'):
                    result.extend([
                        f"✅ **Naming fixes file created:** `{naming_patch_file}`"
                    ])
                
                result.extend([
                    "",
                    "⚠️ **Important:** Review and test all patches in a development environment before applying to production!",
                    ""
                ])
            
            if not any([index_report, performance_report, schema_report, naming_analysis.get('issues')]):
                result.extend([
                    "## ✅ All Clear!",
                    "No issues found in any analysis category. Your database appears to be well-optimized and compliant with best practices.",
                    ""
                ])
            
            return "\n".join(result)
            
        except Exception as e:
            return f"❌ Error during comprehensive analysis: {str(e)}"

    async def generate_patches(self, cursor, db_name: str, patch_type: str = "comprehensive") -> str:
        """Generate SQL patches for database issues."""
        try:
            if patch_type == "comprehensive":
                raw_index_data = index_analyzer.analyze_indexes(cursor, db_name)
                performance_report = performance_analyzer.analyze_performance(cursor, db_name)
                schema_report = schema_analyzer.analyze_schema(cursor, db_name)
                
                if not any([raw_index_data, performance_report, schema_report]):
                    return "✅ No issues found - no patches needed!"
                
                patch_file = patch_generator.generate_comprehensive_patch(
                    raw_index_data, schema_report, performance_report, db_name
                )
                
            elif patch_type == "index":
                raw_index_data = index_analyzer.analyze_indexes(cursor, db_name)
                if not raw_index_data:
                    return "✅ No index issues found - no patches needed!"
                patch_file = patch_generator.save_patch_file(
                    patch_generator.generate_index_patches(raw_index_data),
                    patch_generator.generate_patch_filename(db_name, "index"),
                    db_name
                )
                
            elif patch_type == "performance":
                performance_report = performance_analyzer.analyze_performance(cursor, db_name)
                if not performance_report:
                    return "✅ No performance issues found - no patches needed!"
                patch_file = patch_generator.save_patch_file(
                    patch_generator.generate_performance_patches(performance_report),
                    patch_generator.generate_patch_filename(db_name, "performance"),
                    db_name
                )
                
            elif patch_type == "schema":
                schema_report = schema_analyzer.analyze_schema(cursor, db_name)
                if not schema_report:
                    return "✅ No schema issues found - no patches needed!"
                patch_file = patch_generator.save_patch_file(
                    patch_generator.generate_schema_patches(schema_report),
                    patch_generator.generate_patch_filename(db_name, "schema"),
                    db_name
                )
            
            else:
                return f"❌ Error: Unknown patch type '{patch_type}'"
            
            return f"""🔧 **SQL Patches Generated Successfully**

**Patch File:** `{patch_file}`
**Type:** {patch_type.title()}
**Database:** `{db_name}`

⚠️ **Before applying these patches:**
1. **Backup your database**
2. **Review each SQL statement carefully**
3. **Test in development environment first**
4. **Apply during maintenance window**
5. **Monitor performance after changes**

The patch file contains detailed comments explaining each change."""
            
        except Exception as e:
            return f"❌ Error generating patches: {str(e)}"

    async def run(self):
        """Run the MCP server."""
        logger.info("🚀 Starting MySQL Analyzer MCP Server...")
        logger.info("💡 Send JSON-RPC messages via stdin to interact with the server")
        
        try:
            while True:
                # Read line from stdin
                line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
                
                if not line:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Parse JSON-RPC request
                    request = json.loads(line)
                    
                    # Handle request
                    response = await self.handle_request(request)
                    
                    # Send response
                    print(json.dumps(response), flush=True)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32700,
                            "message": f"Parse error: {str(e)}"
                        }
                    }
                    print(json.dumps(error_response), flush=True)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Server stopped by user")
        except Exception as e:
            logger.error(f"❌ Server error: {e}")


async def main():
    """Main entry point."""
    server = SimpleMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())
