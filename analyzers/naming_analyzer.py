"""
MySQL Database Naming Convention Analyzer and Enforcer

This module analyzes and enforces consistent naming conventions across
MySQL database tables, columns, indexes, and constraints.

Table naming: CamelCase (首字大寫其他小寫)
Column naming: snake_case
"""

import logging
import collections
from typing import Dict, List, Any, Optional
import re

from .utils import get_all_tables, get_table_columns, get_table_indexes

# Configure logging
logger = logging.getLogger(__name__)

# Updated naming conventions
NAMING_CONVENTIONS = {
    'table': {
        'pattern': r'^[A-Z][a-zA-Z0-9]*$',
        'description': 'Tables should use CamelCase (first letter uppercase, e.g., UserProfiles)',
        'examples': ['Users', 'UserProfiles', 'OrderItems', 'ProductCategories']
    },
    'column': {
        'pattern': r'^[a-z][a-z0-9_]*[a-z0-9]$',
        'description': 'Columns should use snake_case (lowercase with underscores)',
        'examples': ['id', 'user_id', 'created_at', 'email_address']
    },
    'primary_key': {
        'pattern': r'^id$',
        'description': 'Primary keys should be named "id"',
        'examples': ['id']
    },
    'foreign_key': {
        'pattern': r'^[a-z][a-z0-9_]*_id$',
        'description': 'Foreign keys should end with "_id" in snake_case',
        'examples': ['user_id', 'order_id', 'category_id']
    },
    'index': {
        'unique': {
            'pattern': r'^uk_[a-z][a-z0-9_]*$',
            'description': 'Unique indexes should start with "uk_" in snake_case',
            'examples': ['uk_user_email', 'uk_product_sku']
        },
        'regular': {
            'pattern': r'^idx_[a-z][a-z0-9_]*$',
            'description': 'Regular indexes should start with "idx_" in snake_case',
            'examples': ['idx_user_status', 'idx_created_at']
        },
        'foreign_key': {
            'pattern': r'^fk_[a-z][a-z0-9_]*$',
            'description': 'Foreign key indexes should start with "fk_" in snake_case',
            'examples': ['fk_user_category', 'fk_order_user']
        }
    },
    'constraint': {
        'check': {
            'pattern': r'^ck_[a-z][a-z0-9_]*$',
            'description': 'Check constraints should start with "ck_" in snake_case',
            'examples': ['ck_age_positive', 'ck_status_valid']
        },
        'foreign_key': {
            'pattern': r'^fk_[a-z][a-z0-9_]*$',
            'description': 'Foreign key constraints should start with "fk_" in snake_case',
            'examples': ['fk_user_category', 'fk_order_user']
        },
        'unique': {
            'pattern': r'^uk_[a-z][a-z0-9_]*$',
            'description': 'Unique constraints should start with "uk_" in snake_case',
            'examples': ['uk_user_email', 'uk_product_sku']
        }
    }
}


def standardize_table_name(name: str) -> str:
    """Convert a name to CamelCase format for tables."""
    # Remove special characters and split on common separators
    name = re.sub(r'[^a-zA-Z0-9_\s-]', '', name)
    
    # Split on underscores, hyphens, or spaces
    parts = re.split(r'[_\s-]+', name)
    
    # Convert each part to title case and join
    camel_case = ''.join(part.capitalize() for part in parts if part)
    
    # Ensure it starts with uppercase
    if camel_case and not camel_case[0].isupper():
        camel_case = camel_case[0].upper() + camel_case[1:]
    
    # If empty or invalid, provide default
    if not camel_case or not camel_case[0].isalpha():
        camel_case = 'Table' + camel_case
    
    return camel_case

def standardize_column_name(name: str) -> str:
    """Convert a name to snake_case format for columns."""
    # Convert camelCase and PascalCase to snake_case
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    
    # Convert to lowercase
    name = name.lower()
    
    # Replace special characters with underscores
    name = re.sub(r'[^a-z0-9_]', '_', name)
    
    # Replace multiple underscores with single underscore
    name = re.sub(r'_+', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    # Ensure it starts with a letter
    if name and not name[0].isalpha():
        name = 'col_' + name
    
    return name

def standardize_foreign_key_name(name: str) -> str:
    """Convert a foreign key name to standard format."""
    name = standardize_column_name(name)
    if not name.endswith('_id'):
        name = name + '_id'
    return name

def generate_index_name(prefix: str, table_name: str, columns: List[str]) -> str:
    """Generate a standardized index name."""
    # Convert table name to snake_case for index naming
    table_snake = standardize_column_name(table_name)
    column_part = '_'.join(standardize_column_name(col) for col in columns if col)
    suggested_name = f"{prefix}{table_snake}_{column_part}"
    
    # Truncate if too long (MySQL index name limit is 64 characters)
    if len(suggested_name) > 64:
        # Keep prefix and truncate the rest
        max_length = 64 - len(prefix)
        table_part = table_snake[:max_length//2]
        column_part = column_part[:max_length//2]
        suggested_name = f"{prefix}{table_part}_{column_part}"
        if len(suggested_name) > 64:
            suggested_name = suggested_name[:60] + '_etc'
    
    return suggested_name

def check_table_naming(table_name: str) -> Optional[Dict[str, Any]]:
    """Check if table name follows CamelCase convention."""
    pattern = NAMING_CONVENTIONS['table']['pattern']
    if not re.match(pattern, table_name):
        suggested_name = standardize_table_name(table_name)
        return {
            'type': 'RENAME_TABLE',
            'severity': 'medium',
            'current_name': table_name,
            'suggested_name': suggested_name,
            'description': f"Table '{table_name}' doesn't follow CamelCase convention. {NAMING_CONVENTIONS['table']['description']}",
            'reason': NAMING_CONVENTIONS['table']['description']
        }
    return None

def check_column_naming(table_name: str, columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Check if column names follow snake_case conventions."""
    issues = []
    
    for column in columns:
        column_name = column['name']
        column_key = column['key']
        
        # Check primary key naming
        if column_key == 'PRI':
            if not re.match(NAMING_CONVENTIONS['primary_key']['pattern'], column_name):
                issues.append({
                    'type': 'RENAME_COLUMN',
                    'severity': 'medium',
                    'table': table_name,
                    'current_name': column_name,
                    'suggested_name': 'id',
                    'description': f"Primary key column '{column_name}' should be named 'id'",
                    'reason': NAMING_CONVENTIONS['primary_key']['description']
                })
            continue
        
        # Check foreign key naming
        if column_name.endswith('_id') or 'id' in column_name.lower():
            if not re.match(NAMING_CONVENTIONS['foreign_key']['pattern'], column_name):
                suggested_name = standardize_foreign_key_name(column_name)
                issues.append({
                    'type': 'RENAME_COLUMN',
                    'severity': 'medium',
                    'table': table_name,
                    'current_name': column_name,
                    'suggested_name': suggested_name,
                    'description': f"Foreign key column '{column_name}' doesn't follow snake_case convention",
                    'reason': NAMING_CONVENTIONS['foreign_key']['description']
                })
            continue
        
        # Check regular column naming
        if not re.match(NAMING_CONVENTIONS['column']['pattern'], column_name):
            suggested_name = standardize_column_name(column_name)
            issues.append({
                'type': 'RENAME_COLUMN',
                'severity': 'low',
                'table': table_name,
                'current_name': column_name,
                'suggested_name': suggested_name,
                'description': f"Column '{column_name}' doesn't follow snake_case convention",
                'reason': NAMING_CONVENTIONS['column']['description']
            })
    
    return issues

def check_index_naming(table_name: str, indexes: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Check if index names follow naming conventions."""
    issues = []
    
    for index_name, index_info in indexes.items():
        if index_name == 'PRIMARY':
            continue  # Skip primary key index
        
        is_unique = index_info['unique']
        columns = index_info['columns']
        
        # Determine expected prefix based on index type
        if is_unique:
            expected_pattern = NAMING_CONVENTIONS['index']['unique']['pattern']
            prefix = 'uk_'
            description = NAMING_CONVENTIONS['index']['unique']['description']
        else:
            # Check if this might be a foreign key index
            if len(columns) == 1 and columns[0].endswith('_id'):
                expected_pattern = NAMING_CONVENTIONS['index']['foreign_key']['pattern']
                prefix = 'fk_'
                description = NAMING_CONVENTIONS['index']['foreign_key']['description']
            else:
                expected_pattern = NAMING_CONVENTIONS['index']['regular']['pattern']
                prefix = 'idx_'
                description = NAMING_CONVENTIONS['index']['regular']['description']
        
        if not re.match(expected_pattern, index_name):
            suggested_name = generate_index_name(prefix, table_name, columns)
            issues.append({
                'type': 'RENAME_INDEX',
                'severity': 'low',
                'table': table_name,
                'current_name': index_name,
                'suggested_name': suggested_name,
                'description': f"Index '{index_name}' doesn't follow naming convention",
                'reason': description
            })
    
    return issues

def analyze_naming_conventions(cursor, db_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Comprehensive analysis of database naming conventions.
    
    Args:
        cursor: MySQL database cursor
        db_name: Name of the database to analyze
        
    Returns:
        dict: Analysis report with naming issues grouped by table
    """
    report = {}
    tables = get_all_tables(cursor, db_name)
    
    for table in tables:
        table_issues = []
        
        # Check table name (CamelCase)
        table_issue = check_table_naming(table)
        if table_issue:
            table_issues.append(table_issue)
        
        # Check column names (snake_case)
        columns = get_table_columns(cursor, db_name, table)
        column_issues = check_column_naming(table, columns)
        table_issues.extend(column_issues)
        
        # Check index names (snake_case with prefixes)
        indexes = get_table_indexes(cursor, db_name, table)
        index_issues = check_index_naming(table, indexes)
        table_issues.extend(index_issues)
        
        if table_issues:
            report[table] = table_issues
    
    return report

def generate_naming_fix_sql(
    cursor, issues: Dict[str, List[Dict[str, Any]]], db_name: str
) -> List[str]:
    """
    Generate SQL statements to fix naming convention issues.
    This version fetches column definitions to create complete ALTER statements.
    """
    sql_statements = [
        f"-- Naming Convention Fixes for Database: {db_name}",
        f"-- Generated on: {__import__('datetime').datetime.now()}",
        "-- Conventions: Tables=CamelCase, Columns=snake_case",
        "-- ⚠️ IMPORTANT: Review and test these changes before applying!",
        f"\nUSE `{db_name}`;\n",
    ]

    # Fetch all column definitions once to avoid repeated queries
    all_columns = {}
    for table in issues.keys():
        all_columns[table] = {
            col["name"]: col for col in get_table_columns(cursor, db_name, table)
        }

    # Group fixes by type
    fix_groups = collections.defaultdict(list)
    for table, table_issues in issues.items():
        for issue in table_issues:
            fix_groups[issue["type"]].append(issue)

    # Generate SQL for each group
    if fix_groups["RENAME_TABLE"]:
        sql_statements.append("-- ===== TABLE RENAMES (CamelCase) =====\n")
        for issue in fix_groups["RENAME_TABLE"]:
            sql_statements.append(
                f"-- {issue['description']}\n"
                f"RENAME TABLE `{issue['current_name']}` TO `{issue['suggested_name']}`;\n"
            )

    if fix_groups["RENAME_INDEX"]:
        sql_statements.append("-- ===== INDEX RENAMES (snake_case with prefixes) =====\n")
        for issue in fix_groups["RENAME_INDEX"]:
            sql_statements.append(
                f"-- {issue['description']}\n"
                f"ALTER TABLE `{issue['table']}` RENAME INDEX `{issue['current_name']}` TO `{issue['suggested_name']}`;\n"
            )

    if fix_groups["RENAME_COLUMN"]:
        sql_statements.append("-- ===== COLUMN RENAMES (snake_case) =====\n")
        for issue in fix_groups["RENAME_COLUMN"]:
            table = issue["table"]
            current_name = issue["current_name"]
            suggested_name = issue["suggested_name"]
            
            col_def = all_columns.get(table, {}).get(current_name)
            if not col_def:
                sql_statements.append(
                    f"-- COULD NOT GENERATE FIX for column '{current_name}' in table '{table}'. Definition not found.\n"
                )
                continue

            # Reconstruct the column definition string
            definition = f"{col_def['data_type']}"
            if col_def["is_nullable"] == "NO":
                definition += " NOT NULL"
            if col_def["default"] is not None:
                default_value = col_def["default"]
                if isinstance(default_value, str):
                    definition += f" DEFAULT '{default_value}'"
                else:
                    definition += f" DEFAULT {default_value}"
            if col_def["extra"]:
                definition += f" {col_def['extra']}"
            if col_def["comment"]:
                definition += f" COMMENT '{col_def['comment']}'"

            sql_statements.append(
                f"-- {issue['description']}\n"
                f"ALTER TABLE `{table}` CHANGE COLUMN `{current_name}` `{suggested_name}` {definition};\n"
            )
            
    return sql_statements

def run_naming_analysis(cursor, db_name: str) -> Dict[str, Any]:
    """
    Main entry point for naming convention analysis.
    
    Args:
        cursor: MySQL database cursor
        db_name: Name of the database to analyze
        
    Returns:
        dict: Comprehensive naming analysis report
    """
    try:
        logger.info(f"Starting naming convention analysis for database: {db_name}")
        
        # Run the analysis
        issues = analyze_naming_conventions(cursor, db_name)
        
        # Count issues by severity
        critical_count = 0
        medium_count = 0
        low_count = 0
        
        for table_issues in issues.values():
            for issue in table_issues:
                severity = issue.get('severity', 'low')
                if severity == 'critical':
                    critical_count += 1
                elif severity == 'medium':
                    medium_count += 1
                else:
                    low_count += 1
        
        # Create summary
        summary = {
            'total_tables_analyzed': len(get_all_tables(cursor, db_name)),
            'tables_with_issues': len(issues),
            'total_issues': critical_count + medium_count + low_count,
            'critical_issues': critical_count,
            'medium_issues': medium_count,
            'low_issues': low_count
        }
        
        logger.info(f"Naming analysis completed. Found {summary['total_issues']} issues across {summary['tables_with_issues']} tables")
        
        return {
            'summary': summary,
            'issues': issues,
            'conventions': NAMING_CONVENTIONS
        }
        
    except Exception as e:
        logger.error(f"Error during naming convention analysis: {e}")
        raise

def format_naming_report(analysis_result: Dict[str, Any]) -> str:
    """
    Format the naming analysis result into a readable report.
    
    Args:
        analysis_result: Result from run_naming_analysis
        
    Returns:
        str: Formatted report
    """
    summary = analysis_result['summary']
    issues = analysis_result['issues']
    
    report = []
    report.append("🏷️  **MySQL Naming Convention Analysis Report**")
    report.append("=" * 50)
    report.append("**Conventions: Tables=CamelCase, Columns=snake_case**")
    report.append("")
    
    # Summary section
    report.append("## 📊 Summary")
    report.append(f"- Tables Analyzed: {summary['total_tables_analyzed']}")
    report.append(f"- Tables with Issues: {summary['tables_with_issues']}")
    report.append(f"- Total Issues: {summary['total_issues']}")
    report.append(f"  - 🔴 Critical: {summary['critical_issues']}")
    report.append(f"  - 🟡 Medium: {summary['medium_issues']}")
    report.append(f"  - 🟢 Low: {summary['low_issues']}")
    report.append("")
    
    if not issues:
        report.append("## ✅ All Clear!")
        report.append("All tables, columns, and indexes follow proper naming conventions.")
        return "\n".join(report)
    
    # Detailed issues
    report.append("## 🔍 Detailed Issues")
    
    for table, table_issues in issues.items():
        report.append(f"### Table: `{table}`")
        
        for issue in table_issues:
            severity_emoji = {"critical": "🔴", "medium": "🟡", "low": "🟢"}.get(issue['severity'], "ℹ️")
            report.append(f"{severity_emoji} **{issue['severity'].upper()}**: {issue['description']}")
            
            if issue['type'] in ['RENAME_TABLE', 'RENAME_COLUMN', 'RENAME_INDEX']:
                report.append(f"   - Current: `{issue['current_name']}`")
                report.append(f"   - Suggested: `{issue['suggested_name']}`")
            
            report.append("")
    
    # Conventions reference
    report.append("## 📋 Naming Conventions Reference")
    report.append("For future reference, here are the enforced naming conventions:")
    report.append("")
    report.append("- **Tables**: CamelCase (e.g., `UserProfiles`, `OrderItems`)")
    report.append("- **Columns**: snake_case (e.g., `user_id`, `created_at`)")
    report.append("- **Primary Keys**: `id`")
    report.append("- **Foreign Keys**: `{table}_id` in snake_case (e.g., `user_id`)")
    report.append("- **Indexes**:")
    report.append("  - Unique: `uk_{table}_{column}` (e.g., `uk_user_email`)")
    report.append("  - Regular: `idx_{table}_{column}` (e.g., `idx_user_status`)")
    report.append("  - Foreign Key: `fk_{table}_{reference}` (e.g., `fk_user_category`)")
    
    return "\n".join(report)
