"""
Enhanced MySQL Index Analyzer with Latest Standards

Analyzes index naming conventions, redundancy, and performance issues
following MySQL 8.0+ best practices and latest naming conventions.
"""

import logging
from typing import Dict, List, Any

from .utils import get_all_tables, get_table_indexes

logger = logging.getLogger(__name__)

def analyze_index_naming_conventions(table: str, indexes: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Analyze index naming conventions following modern MySQL standards.
    
    Modern conventions:
    - Unique indexes: uk_{table}_{columns}
    - Regular indexes: idx_{table}_{columns}  
    - Foreign key indexes: fk_{table}_{reference}
    """
    issues = []
    
    for name, info in indexes.items():
        if name == 'PRIMARY':
            continue
        
        is_unique = info['unique']
        columns = [col for col in info['columns'] if col]
        
        # Determine expected prefix based on modern conventions
        if is_unique:
            expected_prefix = 'uk_'
            convention_desc = "Unique indexes should start with 'uk_' (uk_{table}_{columns})"
        elif len(columns) == 1 and columns[0].endswith('_id'):
            # Likely a foreign key index
            expected_prefix = 'fk_'
            convention_desc = "Foreign key indexes should start with 'fk_' (fk_{table}_{reference})"
        else:
            expected_prefix = 'idx_'
            convention_desc = "Regular indexes should start with 'idx_' (idx_{table}_{columns})"
        
        if not name.startswith(expected_prefix):
            # Generate suggested name following modern conventions
            column_part = '_'.join(columns)
            new_name = f"{expected_prefix}{table}_{column_part}"
            
            # Handle MySQL's 64-character limit for index names
            if len(new_name) > 64:
                # Truncate intelligently
                available_chars = 64 - len(expected_prefix) - 1  # -1 for underscore
                table_part = table[:available_chars//2]
                column_part = column_part[:available_chars//2]
                new_name = f"{expected_prefix}{table_part}_{column_part}"
                if len(new_name) > 64:
                    new_name = new_name[:60] + '_etc'
            
            issues.append({
                'type': 'RENAME_INDEX',
                'severity': 'low',
                'description': f"Index '{name}' on ({', '.join(columns)}) doesn't follow naming convention. {convention_desc}",
                'data': {
                    'table': table, 
                    'old_name': name, 
                    'new_name': new_name,
                    'reason': convention_desc
                }
            })
    
    return issues

def analyze_index_redundancy(table: str, indexes: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Analyze index redundancy with improved logic for MySQL 8.0+.
    
    An index is redundant if:
    1. Another index covers the same or more columns in the same order
    2. The covering index has equal or higher selectivity
    """
    issues = []
    index_list = sorted(indexes.items(), key=lambda x: len(x[1]['columns']))
    
    for i, (name1, info1) in enumerate(index_list):
        if info1['is_primary']:
            continue
            
        for j, (name2, info2) in enumerate(index_list):
            if i >= j or info2['is_primary']:
                continue
            
            cols1 = [col for col in info1['columns'] if col]
            cols2 = [col for col in info2['columns'] if col]
            
            # Check if index1 is a prefix of index2
            if len(cols1) <= len(cols2) and cols2[:len(cols1)] == cols1:
                # Check uniqueness hierarchy: unique index is not redundant to non-unique
                if info1['unique'] and not info2['unique']:
                    continue
                
                # Consider cardinality for redundancy decision
                card1 = sum(info1['cardinality']) if info1['cardinality'] else 0
                card2 = sum(info2['cardinality']) if info2['cardinality'] else 0
                
                # If the longer index has significantly better cardinality, keep both
                if card2 > card1 * 1.5:  # 50% better cardinality threshold
                    continue
                
                issues.append({
                    'type': 'DROP_INDEX',
                    'severity': 'medium',
                    'description': f"Index '{name1}' ({', '.join(cols1)}) is redundant, covered by '{name2}' ({', '.join(cols2)})",
                    'data': {
                        'table': table, 
                        'index_name': name1,
                        'covered_by': name2,
                        'redundant_columns': cols1,
                        'covering_columns': cols2
                    }
                })
                break  # Don't mark the same index as redundant multiple times
    
    return issues

def analyze_index_performance(
    table: str,
    indexes: Dict[str, Any],
    table_rows: int,
) -> List[Dict[str, Any]]:
    """
    Analyze index performance issues including low cardinality.
    """
    issues = []

    for name, info in indexes.items():
        if name == 'PRIMARY':
            continue
            
        columns = [col for col in info['columns'] if col]
        cardinalities = info['cardinality']
        
        # Check for low cardinality indexes (potential performance issue)
        if table_rows > 1000 and cardinalities:  # Only check for tables with significant data
            avg_cardinality = sum(cardinalities) / len(cardinalities)
            selectivity = avg_cardinality / table_rows if table_rows > 0 else 0
            
            if selectivity < 0.1 and not info['unique']:  # Less than 10% selectivity
                issues.append({
                    'type': 'LOW_CARDINALITY_INDEX',
                    'severity': 'medium',
                    'description': f"Index '{name}' has low selectivity ({selectivity:.2%}) which may hurt performance",
                    'data': {
                        'table': table,
                        'index_name': name,
                        'selectivity': selectivity,
                        'columns': columns,
                        'recommendation': 'Consider dropping or combining with other columns'
                    }
                })
    
    return issues

def analyze_indexes(cursor, db_name: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Comprehensive index analysis with latest MySQL 8.0+ standards.
    """
    logger.info(f"Starting comprehensive index analysis for database: {db_name}")
    
    report = {}
    tables = get_all_tables(cursor, db_name)
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{db_name}`.`{table}`")
            table_rows = cursor.fetchone()[0]
        except Exception:
            table_rows = 0
            
        table_issues = []
        indexes = get_table_indexes(cursor, db_name, table)
        
        # Run all analyses for the table
        table_issues.extend(analyze_index_naming_conventions(table, indexes))
        table_issues.extend(analyze_index_redundancy(table, indexes))
        table_issues.extend(analyze_index_performance(table, indexes, table_rows))
        
        if table_issues:
            # Remove duplicate drop recommendations
            unique_issues = []
            seen_drops = set()
            
            for issue in table_issues:
                if issue['type'] == 'DROP_INDEX':
                    index_name = issue['data']['index_name']
                    if index_name not in seen_drops:
                        unique_issues.append(issue)
                        seen_drops.add(index_name)
                else:
                    unique_issues.append(issue)
            
            report[table] = unique_issues
    
    logger.info(f"Index analysis completed. Analyzed {len(tables)} tables.")
    return report

def run_index_analysis(cursor, db_name: str) -> Dict[str, Any]:
    """
    Main entry point for index analysis.
    """
    try:
        return analyze_indexes(cursor, db_name)
    except Exception as e:
        logger.error(f"Error during index analysis: {e}")
        raise
