"""
SQL Patch Generator for MySQL Analysis Issues

This module generates SQL patches to fix issues identified by the various analyzers.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional


def generate_patch_filename(db_name: str, patch_type: str = "mixed") -> str:
    """
    Generate a timestamped patch filename.
    
    Args:
        db_name: Name of the database
        patch_type: Type of patch (e.g., index, schema, comprehensive)
        
    Returns:
        Formatted filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"patch_{db_name}_{patch_type}_{timestamp}.sql"


def generate_index_patches(issues_data: Dict[str, List[Dict[str, Any]]]) -> List[str]:
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


def generate_schema_patches(issues_data: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    """
    Generate SQL statements for schema-related fixes.
    
    Args:
        issues_data: Dictionary of table issues from schema analyzer
        
    Returns:
        List of SQL statements
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


def generate_performance_patches(issues_data: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    """
    Generate SQL statements for performance-related fixes.
    
    Args:
        issues_data: Dictionary of table issues from performance analyzer
        
    Returns:
        List of SQL statements
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


def generate_comprehensive_patch(
    index_issues: Dict[str, List[Dict[str, Any]]],
    schema_issues: Dict[str, List[Dict[str, Any]]],
    performance_issues: Dict[str, List[Dict[str, Any]]],
    db_name: str,
    save_patch_function: callable,
    workspace_dir: Optional[str] = None,
) -> Optional[str]:
    """
    Generate a comprehensive patch file containing all fixes.
    
    Args:
        index_issues: Index analyzer results
        schema_issues: Schema analyzer results
        performance_issues: Performance analyzer results
        db_name: Database name
        save_patch_function: Function to save the patch file (e.g., from server module)
        workspace_dir: Directory to save the patch file
        
    Returns:
        Path to the generated patch file, or None if no patches were generated.
    """
    all_statements = []
    
    # Generate and collect patches from all analyzers
    patch_sections = {
        "SCHEMA": generate_schema_patches(schema_issues),
        "INDEX": generate_index_patches(index_issues),
        "PERFORMANCE": generate_performance_patches(performance_issues),
    }
    
    for section_name, statements in patch_sections.items():
        if statements:
            all_statements.append(f"-- ============================================")
            all_statements.append(f"-- {section_name} FIXES")
            all_statements.append(f"-- ============================================")
            all_statements.extend(statements)
            
    if not all_statements:
        return None

    # Prepare file content
    header = [
        f"-- MySQL Analysis Patch for database: {db_name}",
        f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "-- WARNING: Review and test these statements before executing in production!",
        f"USE `{db_name}`;\n",
    ]
    
    full_content = "\n".join(header + all_statements)
    
    # Generate filename and save the patch
    filename = generate_patch_filename(db_name, "comprehensive")
    filepath = save_patch_function(full_content, filename, workspace_dir)
    
    return filepath
