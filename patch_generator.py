"""
SQL Patch Generator for MySQL Analysis Issues

This module generates SQL patches to fix issues identified by the various analyzers.
"""

import os
from datetime import datetime


def generate_patch_filename(db_name, patch_type="mixed"):
    """
    Generate a timestamped patch filename.
    
    Args:
        db_name: Name of the database
        patch_type: Type of patch (index, schema, performance, mixed)
        
    Returns:
        str: Formatted filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"patch_{db_name}_{patch_type}_{timestamp}.sql"


def generate_index_patches(issues_data):
    """
    Generate SQL statements for index-related fixes.
    
    Args:
        issues_data: Dictionary of table issues from index analyzer
        
    Returns:
        list: List of SQL statements
    """
    sql_statements = []
    
    for table, issues in issues_data.items():
        for issue in issues:
            if issue['type'] == 'RENAME_INDEX':
                data = issue['data']
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"ALTER TABLE `{data['table']}` RENAME INDEX `{data['old_name']}` TO `{data['new_name']}`;"
                )
            elif issue['type'] == 'DROP_INDEX':
                data = issue['data']
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"ALTER TABLE `{data['table']}` DROP INDEX `{data['index_name']}`;"
                )
    
    return sql_statements


def generate_schema_patches(issues_data):
    """
    Generate SQL statements for schema-related fixes.
    
    Args:
        issues_data: Dictionary of table issues from schema analyzer
        
    Returns:
        list: List of SQL statements
    """
    sql_statements = []
    
    for table, issues in issues_data.items():
        for issue in issues:
            data = issue['data']
            
            if issue['type'] == 'ALTER_ENGINE':
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"ALTER TABLE `{data['table']}` ENGINE=InnoDB;"
                )
            elif issue['type'] == 'ALTER_CHARSET':
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"ALTER TABLE `{data['table']}` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                )
            elif issue['type'] == 'CREATE_INDEX':
                columns_str = ', '.join([f"`{col}`" for col in data['columns']])
                index_name = f"fk_{data['table']}_{'_'.join(data['columns'])}"
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"CREATE INDEX `{index_name}` ON `{data['table']}` ({columns_str});"
                )
            elif issue['type'] == 'ALTER_COLUMN_TYPE':
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"ALTER TABLE `{data['table']}` MODIFY COLUMN `{data['column']}` {data['new_type']};"
                )
    
    return sql_statements


def generate_performance_patches(issues_data):
    """
    Generate SQL statements for performance-related fixes.
    
    Args:
        issues_data: Dictionary of table issues from performance analyzer
        
    Returns:
        list: List of SQL statements
    """
    sql_statements = []
    
    for table, issues in issues_data.items():
        for issue in issues:
            data = issue['data']
            
            if issue['type'] == 'DROP_INDEX':
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"ALTER TABLE `{data['table']}` DROP INDEX `{data['index_name']}`;"
                )
            elif issue['type'] == 'OPTIMIZE_TABLE':
                sql_statements.append(
                    f"-- {issue['description']}\n"
                    f"OPTIMIZE TABLE `{data['table']}`;"
                )
    
    return sql_statements


def save_patch_file(sql_statements, filename, db_name):
    """
    Save SQL statements to a patch file.
    
    Args:
        sql_statements: List of SQL statements
        filename: Name of the patch file
        db_name: Database name
        
    Returns:
        str: Path to the saved file
    """
    if not os.path.exists('patches'):
        os.makedirs('patches')
    
    filepath = os.path.join('patches', filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"-- MySQL Analysis Patch for database: {db_name}\n")
        f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("-- WARNING: Review and test these statements before executing in production!\n\n")
        
        if not sql_statements:
            f.write("-- No issues found that require SQL patches.\n")
        else:
            f.write("USE `" + db_name + "`;\n\n")
            for statement in sql_statements:
                f.write(statement + "\n\n")
    
    return filepath


def generate_comprehensive_patch(index_issues, schema_issues, performance_issues, db_name):
    """
    Generate a comprehensive patch file containing all fixes.
    
    Args:
        index_issues: Index analyzer results
        schema_issues: Schema analyzer results  
        performance_issues: Performance analyzer results
        db_name: Database name
        
    Returns:
        str: Path to the generated patch file
    """
    all_statements = []
    
    # Add schema fixes first (they might affect indexes)
    schema_statements = generate_schema_patches(schema_issues)
    if schema_statements:
        all_statements.append("-- ============================================")
        all_statements.append("-- SCHEMA FIXES")
        all_statements.append("-- ============================================")
        all_statements.extend(schema_statements)
    
    # Add index fixes
    index_statements = generate_index_patches(index_issues)
    if index_statements:
        all_statements.append("-- ============================================")
        all_statements.append("-- INDEX FIXES")  
        all_statements.append("-- ============================================")
        all_statements.extend(index_statements)
    
    # Add performance fixes last
    performance_statements = generate_performance_patches(performance_issues)
    if performance_statements:
        all_statements.append("-- ============================================")
        all_statements.append("-- PERFORMANCE FIXES")
        all_statements.append("-- ============================================")
        all_statements.extend(performance_statements)
    
    filename = generate_patch_filename(db_name, "comprehensive")
    filepath = save_patch_file(all_statements, filename, db_name)
    
    return filepath
