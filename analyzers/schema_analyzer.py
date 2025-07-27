"""
Enhanced MySQL Schema Analyzer with Latest Standards

Analyzes table schema compliance with MySQL 8.0+ best practices,
including engine, charset, constraints, and modern schema patterns.
"""

import collections
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# MySQL 8.0+ recommended settings
RECOMMENDED_SETTINGS = {
    'engine': 'InnoDB',
    'charset': 'utf8mb4',
    'collation_pattern': r'^utf8mb4_',
    'row_format': 'DYNAMIC',
    'max_int_value': 2147483647,
    'max_bigint_value': 9223372036854775807
}

def get_table_status(cursor, db_name: str) -> Dict[str, Dict[str, Any]]:
    """Get comprehensive table status information."""
    query = """
        SELECT 
            TABLE_NAME,
            ENGINE,
            TABLE_COLLATION,
            AUTO_INCREMENT,
            ROW_FORMAT,
            CREATE_OPTIONS,
            TABLE_COMMENT,
            DATA_LENGTH,
            INDEX_LENGTH
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
    """
    cursor.execute(query, (db_name,))
    
    tables_info = {}
    for row in cursor.fetchall():
        table_name = row[0]
        tables_info[table_name] = {
            'engine': row[1],
            'collation': row[2],
            'auto_increment': row[3],
            'row_format': row[4],
            'create_options': row[5] or '',
            'comment': row[6] or '',
            'data_length': row[7] or 0,
            'index_length': row[8] or 0
        }
    
    return tables_info

def get_table_indexes_summary(cursor, db_name: str) -> Dict[str, List[List[str]]]:
    """Get simplified index information for constraint analysis."""
    query = """
        SELECT 
            TABLE_NAME,
            INDEX_NAME,
            GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS COLUMNS
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s
        GROUP BY TABLE_NAME, INDEX_NAME
    """
    cursor.execute(query, (db_name,))
    
    table_indexes = collections.defaultdict(list)
    for row in cursor.fetchall():
        table_name, index_name, columns = row
        column_list = columns.split(',') if columns else []
        table_indexes[table_name].append(column_list)
    
    return dict(table_indexes)

def get_foreign_key_constraints(cursor, db_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """Get all foreign key constraints with detailed information."""
    query = """
        SELECT 
            kcu.TABLE_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME,
            rc.UPDATE_RULE,
            rc.DELETE_RULE
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
            ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
        WHERE kcu.TABLE_SCHEMA = %s 
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
    """
    cursor.execute(query, (db_name,))
    
    foreign_keys = collections.defaultdict(list)
    for row in cursor.fetchall():
        table_name, constraint_name, column_name, ref_table, ref_column, update_rule, delete_rule = row
        foreign_keys[table_name].append({
            'constraint_name': constraint_name,
            'column': column_name,
            'referenced_table': ref_table,
            'referenced_column': ref_column,
            'update_rule': update_rule,
            'delete_rule': delete_rule
        })
    
    return dict(foreign_keys)

def get_table_columns_info(cursor, db_name: str, table_name: str) -> List[Dict[str, Any]]:
    """Get detailed column information for a specific table."""
    query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            EXTRA,
            COLUMN_KEY,
            NUMERIC_PRECISION,
            CHARACTER_MAXIMUM_LENGTH,
            COLUMN_COMMENT
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query, (db_name, table_name))
    
    columns = []
    for row in cursor.fetchall():
        columns.append({
            'name': row[0],
            'data_type': row[1],
            'is_nullable': row[2],
            'default': row[3],
            'extra': row[4] or '',
            'key': row[5] or '',
            'precision': row[6],
            'max_length': row[7],
            'comment': row[8] or ''
        })
    
    return columns

def analyze_table_engine(table_name: str, table_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Analyze table storage engine compliance with modern standards."""
    engine = table_info.get('engine', '').upper()
    
    if engine != RECOMMENDED_SETTINGS['engine'].upper():
        return {
            'type': 'ALTER_ENGINE',
            'severity': 'high',
            'description': f"Table '{table_name}' uses '{engine}' engine. Should use '{RECOMMENDED_SETTINGS['engine']}' for ACID compliance, row-level locking, and better performance.",
            'data': {
                'table': table_name,
                'current_engine': engine,
                'recommended_engine': RECOMMENDED_SETTINGS['engine']
            }
        }
    return None

def analyze_charset_collation(table_name: str, table_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Analyze charset and collation compliance with modern UTF-8 standards."""
    collation = table_info.get('collation', '')
    
    if not collation.startswith('utf8mb4'):
        charset = 'utf8' if collation.startswith('utf8') else 'unknown'
        return {
            'type': 'ALTER_CHARSET',
            'severity': 'medium',
            'description': f"Table '{table_name}' uses '{collation}' collation. Should use 'utf8mb4' charset for full Unicode support including emojis and special characters.",
            'data': {
                'table': table_name,
                'current_collation': collation,
                'current_charset': charset,
                'recommended_charset': RECOMMENDED_SETTINGS['charset'],
                'recommended_collation': 'utf8mb4_unicode_ci'
            }
        }
    return None

def analyze_row_format(table_name: str, table_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Analyze row format for optimal performance in MySQL 8.0+."""
    row_format = table_info.get('row_format', '').upper()
    
    if row_format and row_format not in ['DYNAMIC', 'COMPRESSED']:
        return {
            'type': 'ALTER_ROW_FORMAT',
            'severity': 'low',
            'description': f"Table '{table_name}' uses '{row_format}' row format. Consider 'DYNAMIC' for better performance with variable-length columns.",
            'data': {
                'table': table_name,
                'current_row_format': row_format,
                'recommended_row_format': RECOMMENDED_SETTINGS['row_format']
            }
        }
    return None

def analyze_auto_increment_overflow(table_name: str, table_info: Dict[str, Any], 
                                  columns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Analyze auto-increment columns for potential overflow issues."""
    auto_increment = table_info.get('auto_increment')
    if not auto_increment:
        return None
    
    # Find the auto-increment column
    ai_column = None
    for column in columns:
        if 'auto_increment' in column.get('extra', '').lower():
            ai_column = column
            break
    
    if not ai_column:
        return None
    
    data_type = ai_column['data_type'].upper()
    column_name = ai_column['name']
    
    # Check for different integer types
    if data_type == 'INT':
        max_value = RECOMMENDED_SETTINGS['max_int_value']
        threshold = max_value * 0.7  # 70% threshold
        new_type = 'BIGINT'
    elif data_type == 'SMALLINT':
        max_value = 32767
        threshold = max_value * 0.7
        new_type = 'INT'
    elif data_type == 'TINYINT':
        max_value = 127
        threshold = max_value * 0.7
        new_type = 'SMALLINT'
    else:
        return None  # BIGINT or other types are generally safe
    
    if auto_increment > threshold:
        percentage = (auto_increment / max_value) * 100
        return {
            'type': 'ALTER_COLUMN_TYPE',
            'severity': 'critical' if percentage > 90 else 'high',
            'description': f"Auto-increment column '{column_name}' in table '{table_name}' is at {auto_increment:,} ({percentage:.1f}% of {data_type} max value). Risk of overflow.",
            'data': {
                'table': table_name,
                'column': column_name,
                'current_type': data_type,
                'recommended_type': new_type,
                'current_value': auto_increment,
                'max_value': max_value,
                'percentage_used': percentage
            }
        }
    return None

def analyze_foreign_key_indexes(table_name: str, foreign_keys: List[Dict[str, Any]], 
                               table_indexes: List[List[str]]) -> List[Dict[str, Any]]:
    """Analyze foreign key columns for missing indexes."""
    issues = []
    
    for fk in foreign_keys:
        fk_column = fk['column']
        
        # Check if foreign key column is indexed
        is_indexed = False
        for index_columns in table_indexes:
            if index_columns and index_columns[0] == fk_column:
                is_indexed = True
                break
        
        if not is_indexed:
            issues.append({
                'type': 'CREATE_INDEX',
                'severity': 'high',
                'description': f"Foreign key column '{fk_column}' in table '{table_name}' is not indexed. This can cause severe performance issues during parent table operations.",
                'data': {
                    'table': table_name,
                    'column': fk_column,
                    'referenced_table': fk['referenced_table'],
                    'referenced_column': fk['referenced_column'],
                    'suggested_index_name': f"fk_{table_name}_{fk_column.replace('_id', '')}"
                }
            })
    
    return issues

def analyze_table_size_and_performance(table_name: str, table_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Analyze table size and suggest performance optimizations."""
    issues = []
    
    data_length = table_info.get('data_length', 0)
    index_length = table_info.get('index_length', 0)
    total_size = data_length + index_length
    
    # Large table analysis (>1GB)
    if total_size > 1024 * 1024 * 1024:  # 1GB
        size_gb = total_size / (1024 * 1024 * 1024)
        
        # Check index-to-data ratio
        if data_length > 0:
            index_ratio = index_length / data_length
            if index_ratio > 2:  # Indexes are more than 2x the data size
                issues.append({
                    'type': 'OPTIMIZE_INDEXES',
                    'severity': 'medium',
                    'description': f"Table '{table_name}' ({size_gb:.2f}GB) has a high index-to-data ratio ({index_ratio:.2f}). Consider reviewing index usage.",
                    'data': {
                        'table': table_name,
                        'total_size_gb': size_gb,
                        'index_ratio': index_ratio,
                        'recommendation': 'Review and remove unused indexes'
                    }
                })
    
    return issues

def analyze_schema(cursor, db_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Comprehensive schema analysis for MySQL 8.0+ best practices.
    
    Analyzes:
    1. Storage engine compliance (InnoDB)
    2. Charset/collation (utf8mb4)
    3. Row format optimization
    4. Auto-increment overflow risks
    5. Foreign key indexing
    6. Table size and performance
    """
    logger.info(f"Starting comprehensive schema analysis for database: {db_name}")
    
    report = {}
    
    # Get comprehensive table information
    tables_info = get_table_status(cursor, db_name)
    table_indexes = get_table_indexes_summary(cursor, db_name)
    foreign_keys_info = get_foreign_key_constraints(cursor, db_name)
    
    for table_name, table_info in tables_info.items():
        table_issues = []
        
        # 1. Engine Analysis
        engine_issue = analyze_table_engine(table_name, table_info)
        if engine_issue:
            table_issues.append(engine_issue)
        
        # 2. Charset/Collation Analysis
        charset_issue = analyze_charset_collation(table_name, table_info)
        if charset_issue:
            table_issues.append(charset_issue)
        
        # 3. Row Format Analysis
        row_format_issue = analyze_row_format(table_name, table_info)
        if row_format_issue:
            table_issues.append(row_format_issue)
        
        # 4. Auto-increment Overflow Analysis
        columns = get_table_columns_info(cursor, db_name, table_name)
        ai_issue = analyze_auto_increment_overflow(table_name, table_info, columns)
        if ai_issue:
            table_issues.append(ai_issue)
        
        # 5. Foreign Key Index Analysis
        if table_name in foreign_keys_info:
            fk_index_issues = analyze_foreign_key_indexes(
                table_name, 
                foreign_keys_info[table_name],
                table_indexes.get(table_name, [])
            )
            table_issues.extend(fk_index_issues)
        
        # 6. Table Size and Performance Analysis
        size_issues = analyze_table_size_and_performance(table_name, table_info)
        table_issues.extend(size_issues)
        
        if table_issues:
            report[table_name] = table_issues
    
    logger.info(f"Schema analysis completed. Analyzed {len(tables_info)} tables.")
    return report

def run_schema_analysis(cursor, db_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Main entry point for schema analysis.
    Returns analysis results directly for the MCP server.
    """
    try:
        return analyze_schema(cursor, db_name)
    except Exception as e:
        logger.error(f"Error during schema analysis: {e}")
        raise
