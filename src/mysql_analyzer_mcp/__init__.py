"""
MySQL Analyzer MCP Server Package

A Model Context Protocol (MCP) server for comprehensive MySQL database analysis.
"""

__version__ = "1.0.0"
__author__ = "MySQL Analyzer Team"
__description__ = "MySQL Database Analyzer MCP Server with CamelCase tables and snake_case columns"

# Ensure the package can be imported properly
try:
    from . import server
except ImportError:
    # Handle case where the package is imported from different contexts
    pass

__all__ = ["server"]
