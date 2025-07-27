
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def analyze_performance(
    cursor,
    db_name: str,
    min_rows_for_unused_index: int = 1000,
    fragmentation_threshold_mb: int = 10,
    fragmentation_ratio: float = 0.2,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Analyzes performance-related metrics, such as unused indexes and table fragmentation.
    """
    report = {}

    # 1. Unused Indexes Check
    try:
        query_unused_indexes = """
            SELECT s.TABLE_NAME, s.INDEX_NAME
            FROM information_schema.STATISTICS s
            LEFT JOIN performance_schema.table_io_waits_summary_by_index_usage p 
                ON s.TABLE_SCHEMA = p.OBJECT_SCHEMA 
                AND s.TABLE_NAME = p.OBJECT_NAME 
                AND s.INDEX_NAME = p.INDEX_NAME
            WHERE s.TABLE_SCHEMA = %s
              AND s.INDEX_NAME != 'PRIMARY'
              AND (p.COUNT_FETCH IS NULL OR p.COUNT_FETCH = 0)
              AND (SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = s.TABLE_SCHEMA AND TABLE_NAME = s.TABLE_NAME) > %s
        """
        cursor.execute(query_unused_indexes, (db_name, min_rows_for_unused_index))
        for table, index in cursor.fetchall():
            report.setdefault(table, []).append({
                'type': 'DROP_INDEX',
                'severity': 'medium',
                'description': f"Index '{index}' appears to be unused. (Requires performance_schema and long uptime for accuracy).",
                'data': {'table': table, 'index_name': index},
            })
    except Exception as e:
        logger.warning(f"Could not run unused index check (this is expected if performance_schema is disabled): {e}")

    # 2. Table Fragmentation Check
    try:
        query_fragmentation = """
            SELECT TABLE_NAME, DATA_LENGTH, DATA_FREE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND DATA_FREE > %s;
        """
        cursor.execute(query_fragmentation, (db_name, fragmentation_threshold_mb * 1024 * 1024))
        for table, data_length, data_free in cursor.fetchall():
            if data_length > 0 and (data_free / data_length) > fragmentation_ratio:
                report.setdefault(table, []).append({
                    'type': 'OPTIMIZE_TABLE',
                    'severity': 'low',
                    'description': f"Table has significant fragmentation ({data_free / 1024 / 1024:.2f} MB free). Consider running OPTIMIZE TABLE.",
                    'data': {'table': table},
                })
    except Exception as e:
        logger.error(f"Could not run table fragmentation check: {e}")

    return report
