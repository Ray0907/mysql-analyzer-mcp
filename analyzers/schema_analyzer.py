"""
Enhanced MySQL Schema Analyzer with Latest Standards

Analyzes table schema compliance with MySQL 8.0+ best practices,
including engine, charset, constraints, and modern schema patterns.
"""

import logging
from typing import Dict, List, Any, Optional

from .utils import (
    get_table_status,
    get_table_indexes,
    get_foreign_key_constraints,
    get_table_columns,
)

logger = logging.getLogger(__name__)

# MySQL 8.0+ recommended settings
RECOMMENDED_SETTINGS = {
    'engine': 'InnoDB',
    'charset': 'utf8mb4',
    'collation_pattern': r'^utf8mb4_',
    'row_format': 'DYNAMIC',
    'max_int_value': 2147483647,
    'max_bigint_value': 9223372036854775807,
}

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

def analyze_foreign_key_indexes(
    table_name: str,
    foreign_keys: List[Dict[str, Any]],
    table_indexes: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Analyze foreign key columns for missing indexes."""
    issues = []
    
    # Create a set of the first column of all indexes for faster lookups
    indexed_first_columns = {
        info["columns"][0]
        for info in table_indexes.values()
        if info["columns"]
    }

    for fk in foreign_keys:
        fk_column = fk["column"]
        if fk_column not in indexed_first_columns:
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
    """
    logger.info(f"Starting comprehensive schema analysis for database: {db_name}")
    
    report = {}
    tables_info = get_table_status(cursor, db_name)
    foreign_keys_info = get_foreign_key_constraints(cursor, db_name)
    
    for table_name, table_info in tables_info.items():
        table_issues = []
        columns = get_table_columns(cursor, db_name, table_name)
        indexes = get_table_indexes(cursor, db_name, table_name)

        # Run all analyses for the table
        table_issues.extend(
            filter(None, [
                analyze_table_engine(table_name, table_info),
                analyze_charset_collation(table_name, table_info),
                analyze_row_format(table_name, table_info),
                analyze_auto_increment_overflow(table_name, table_info, columns),
            ])
        )
        table_issues.extend(
            analyze_foreign_key_indexes(
                table_name,
                foreign_keys_info.get(table_name, []),
                indexes,
            )
        )
        table_issues.extend(analyze_table_size_and_performance(table_name, table_info))
        
        if table_issues:
            report[table_name] = table_issues
            
    logger.info(f"Schema analysis completed. Analyzed {len(tables_info)} tables.")
    return report
