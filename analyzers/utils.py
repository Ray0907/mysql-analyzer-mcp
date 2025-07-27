"""
Database Analyzer Utilities

This module provides shared utility functions for database analysis,
including fetching table metadata, column details, and index information.
"""

import collections
from typing import Dict, List, Any

def get_all_tables(cursor, db_name: str) -> List[str]:
    """Fetch all tables from the database."""
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (db_name,))
    return [item[0] for item in cursor.fetchall()]

def get_table_columns(cursor, db_name: str, table_name: str) -> List[Dict[str, Any]]:
    """Fetch all columns for a given table with their properties."""
    cursor.execute("""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            extra,
            column_key,
            column_comment
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (db_name, table_name))
    
    columns = []
    for row in cursor.fetchall():
        columns.append({
            'name': row[0],
            'data_type': row[1],
            'is_nullable': row[2],
            'default': row[3],
            'extra': row[4],
            'key': row[5],
            'comment': row[6]
        })
    return columns

def get_table_indexes(cursor, db_name: str, table_name: str) -> Dict[str, Any]:
    """Fetch all indexes for a given table, including column order and uniqueness."""
    query = """
        SELECT 
            INDEX_NAME, 
            COLUMN_NAME, 
            NON_UNIQUE,
            SEQ_IN_INDEX,
            INDEX_TYPE,
            INDEX_COMMENT,
            CARDINALITY
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
    """
    cursor.execute(query, (db_name, table_name))
    
    indexes = collections.defaultdict(lambda: {
        'columns': [], 
        'unique': True, 
        'is_primary': False,
        'type': 'BTREE',
        'comment': '',
        'cardinality': []
    })
    
    rows = cursor.fetchall()
    for row in rows:
        index_name, column_name, non_unique, seq_in_index, index_type, comment, cardinality = row
        
        # Store columns in correct order
        while len(indexes[index_name]['columns']) < seq_in_index:
            indexes[index_name]['columns'].append(None)
            indexes[index_name]['cardinality'].append(None)
            
        indexes[index_name]['columns'][seq_in_index - 1] = column_name
        indexes[index_name]['cardinality'][seq_in_index - 1] = cardinality or 0

        indexes[index_name]['unique'] = (non_unique == 0)
        indexes[index_name]['is_primary'] = (index_name == 'PRIMARY')
        indexes[index_name]['type'] = index_type or 'BTREE'
        indexes[index_name]['comment'] = comment or ''

    return dict(indexes)

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
