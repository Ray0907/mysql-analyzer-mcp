
def analyze_performance(cursor, db_name):
    """
    Analyzes performance-related metrics.
    Checks for: Unused indexes and table fragmentation.
    
    NOTE: Unused index check requires performance_schema to be enabled and the
    server to have been running for a significant time to gather meaningful data.
    """
    report = {}
    
    # --- 1. Unused Indexes Check ---
    # This query checks for indexes that have never been used.
    query_unused_indexes = """
        SELECT 
            s.TABLE_NAME, 
            s.INDEX_NAME
        FROM information_schema.STATISTICS s
        LEFT JOIN information_schema.TABLE_STATISTICS ts ON s.TABLE_SCHEMA = ts.TABLE_SCHEMA AND s.TABLE_NAME = ts.TABLE_NAME
        LEFT JOIN performance_schema.table_io_waits_summary_by_index_usage p ON s.TABLE_SCHEMA = p.OBJECT_SCHEMA AND s.TABLE_NAME = p.OBJECT_NAME AND s.INDEX_NAME = p.INDEX_NAME
        WHERE s.TABLE_SCHEMA = %s
          AND s.INDEX_NAME != 'PRIMARY'
          AND (p.COUNT_FETCH IS NULL OR p.COUNT_FETCH = 0)
          AND ts.ROWS_READ > 1000; -- Only consider tables with some activity
    """
    try:
        cursor.execute("SET @db_name = %s", (db_name,))
        cursor.execute(query_unused_indexes, (db_name,))
        
        for row in cursor.fetchall():
            table, index = row
            if table not in report:
                report[table] = []
            
            report[table].append({
                'type': 'DROP_INDEX',
                'severity': 'medium',
                'description': f"Index '{index}' appears to be unused. (Requires performance_schema and long uptime for accuracy).",
                'data': {'table': table, 'index_name': index}
            })
    except Exception as e:
        # This can fail if performance_schema is not enabled.
        print(f"\n[Warning] Could not run unused index check. Error: {e}")
        print("Please ensure performance_schema is enabled and the user has appropriate permissions.")


    # --- 2. Table Fragmentation Check ---
    query_fragmentation = """
        SELECT 
            TABLE_NAME,
            DATA_LENGTH,
            DATA_FREE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND DATA_FREE > 0;
    """
    cursor.execute(query_fragmentation, (db_name,))
    
    for row in cursor.fetchall():
        table, data_length, data_free = row
        
        # Warn if fragmentation is more than 20% of the table's data size, and is significant (> 10MB)
        if data_length > 0 and data_free > 10 * 1024 * 1024 and (data_free / data_length) > 0.2:
            if table not in report:
                report[table] = []
            
            report[table].append({
                'type': 'OPTIMIZE_TABLE',
                'severity': 'low',
                'description': f"Table has significant fragmentation ({data_free / 1024 / 1024:.2f} MB free). Consider running OPTIMIZE TABLE.",
                'data': {'table': table}
            })

    return report
