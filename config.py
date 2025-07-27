#!/usr/bin/env python3
"""
MySQL Analyzer MCP Configuration Manager

Handles configuration loading from .env files and provides
centralized settings for analysis filters, output formats,
and optimization preferences.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass
class AnalysisConfig:
    """Configuration for analysis behavior and output formatting."""
    
    # 過濾和優先級設定
    min_severity: str = "low"  # low, medium, high, critical
    show_only_issues: bool = False  # 只列出有問題的
    dedup_suggestions: bool = True  # 重複建議刪除
    simplify_low_value_info: bool = True  # 低價值資訊簡化
    
    # 輸出格式選項
    output_format: str = "markdown"  # markdown, json, text
    include_summaries: bool = True
    include_detailed_explanations: bool = False
    max_issues_per_table: int = 10
    
    # 表格關聯分析
    enable_table_relationship_analysis: bool = True
    analyze_foreign_key_chains: bool = True
    detect_orphaned_tables: bool = True
    
    # 報告優化
    group_by_severity: bool = True
    show_statistics: bool = True
    include_recommendations: bool = True
    
    # SQL 補丁生成
    generate_backup_statements: bool = True
    include_rollback_scripts: bool = True
    max_patch_size_mb: int = 10


@dataclass  
class DatabaseConfig:
    """Database connection configuration."""
    
    host: str = "localhost"
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    
    # 連接池設定
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    
    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> 'DatabaseConfig':
        """從 .env 檔案載入資料庫配置."""
        if env_file:
            load_dotenv(env_file)
        else:
            # 嘗試載入多個位置的 .env 檔案
            for env_path in ['.env', '.env.local', '.env.dev']:
                if os.path.exists(env_path):
                    load_dotenv(env_path)
                    logger.info(f"載入配置檔案: {env_path}")
                    break
        
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '3306')),
            user=os.getenv('DB_USER', ''),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_DATABASE', ''),
            charset=os.getenv('DB_CHARSET', 'utf8mb4'),
            pool_size=int(os.getenv('DB_POOL_SIZE', '5')),
            max_overflow=int(os.getenv('DB_MAX_OVERFLOW', '10')),
            pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', '30'))
        )
    
    def is_valid(self) -> bool:
        """檢查配置是否有效."""
        return bool(self.user and self.password and self.database)


class ConfigManager:
    """集中化的配置管理器."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or '.env'
        self._db_config = None
        self._analysis_config = None
        self._load_configs()
    
    def _load_configs(self):
        """載入所有配置."""
        # 載入資料庫配置
        self._db_config = DatabaseConfig.from_env(self.config_file)
        
        # 載入分析配置
        self._analysis_config = self._load_analysis_config()
        
        # 驗證配置
        if not self._db_config.is_valid():
            logger.warning("資料庫配置不完整，請檢查 .env 檔案")
    
    def _load_analysis_config(self) -> AnalysisConfig:
        """從環境變數載入分析配置."""
        return AnalysisConfig(
            min_severity=os.getenv('ANALYSIS_MIN_SEVERITY', 'low'),
            show_only_issues=os.getenv('ANALYSIS_SHOW_ONLY_ISSUES', 'false').lower() == 'true',
            dedup_suggestions=os.getenv('ANALYSIS_DEDUP_SUGGESTIONS', 'true').lower() == 'true',
            simplify_low_value_info=os.getenv('ANALYSIS_SIMPLIFY_LOW_VALUE', 'true').lower() == 'true',
            output_format=os.getenv('ANALYSIS_OUTPUT_FORMAT', 'markdown'),
            include_summaries=os.getenv('ANALYSIS_INCLUDE_SUMMARIES', 'true').lower() == 'true',
            include_detailed_explanations=os.getenv('ANALYSIS_DETAILED_EXPLANATIONS', 'false').lower() == 'true',
            max_issues_per_table=int(os.getenv('ANALYSIS_MAX_ISSUES_PER_TABLE', '10')),
            enable_table_relationship_analysis=os.getenv('ANALYSIS_TABLE_RELATIONSHIPS', 'true').lower() == 'true',
            analyze_foreign_key_chains=os.getenv('ANALYSIS_FK_CHAINS', 'true').lower() == 'true',
            detect_orphaned_tables=os.getenv('ANALYSIS_DETECT_ORPHANED', 'true').lower() == 'true',
            group_by_severity=os.getenv('ANALYSIS_GROUP_BY_SEVERITY', 'true').lower() == 'true',
            show_statistics=os.getenv('ANALYSIS_SHOW_STATS', 'true').lower() == 'true',
            include_recommendations=os.getenv('ANALYSIS_INCLUDE_RECOMMENDATIONS', 'true').lower() == 'true',
            generate_backup_statements=os.getenv('ANALYSIS_GENERATE_BACKUPS', 'true').lower() == 'true',
            include_rollback_scripts=os.getenv('ANALYSIS_INCLUDE_ROLLBACKS', 'true').lower() == 'true',
            max_patch_size_mb=int(os.getenv('ANALYSIS_MAX_PATCH_SIZE_MB', '10'))
        )
    
    @property
    def db_config(self) -> DatabaseConfig:
        """取得資料庫配置."""
        return self._db_config
    
    @property
    def analysis_config(self) -> AnalysisConfig:
        """取得分析配置."""
        return self._analysis_config
    
    def reload(self):
        """重新載入配置."""
        self._load_configs()
        logger.info("配置已重新載入")
    
    def get_db_connection_args(self) -> Dict[str, Any]:
        """取得資料庫連接參數."""
        return {
            'host': self.db_config.host,
            'port': self.db_config.port,
            'user': self.db_config.user,
            'password': self.db_config.password,
            'database': self.db_config.database,
            'charset': self.db_config.charset
        }
    
    def override_db_config(self, **kwargs) -> DatabaseConfig:
        """使用提供的參數覆蓋資料庫配置."""
        config_dict = {
            'host': kwargs.get('db_host', self.db_config.host),
            'port': kwargs.get('db_port', self.db_config.port),
            'user': kwargs.get('db_user', self.db_config.user),
            'password': kwargs.get('db_password', self.db_config.password),
            'database': kwargs.get('db_database', self.db_config.database),
            'charset': kwargs.get('db_charset', self.db_config.charset)
        }
        return DatabaseConfig(**config_dict)


# 全局配置管理器實例
config_manager = ConfigManager()


def get_config() -> ConfigManager:
    """取得全局配置管理器."""
    return config_manager


def create_config_template(filepath: str = '.env.example'):
    """創建配置檔案範本."""
    template = '''# MySQL Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_DATABASE=your_database
DB_CHARSET=utf8mb4

# Connection Pool Settings
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT=30

# Analysis Configuration
# Filtering and Priority
ANALYSIS_MIN_SEVERITY=low
ANALYSIS_SHOW_ONLY_ISSUES=false
ANALYSIS_DEDUP_SUGGESTIONS=true
ANALYSIS_SIMPLIFY_LOW_VALUE=true

# Output Format Options
ANALYSIS_OUTPUT_FORMAT=markdown
ANALYSIS_INCLUDE_SUMMARIES=true
ANALYSIS_DETAILED_EXPLANATIONS=false
ANALYSIS_MAX_ISSUES_PER_TABLE=10

# Table Relationship Analysis
ANALYSIS_TABLE_RELATIONSHIPS=true
ANALYSIS_FK_CHAINS=true
ANALYSIS_DETECT_ORPHANED=true

# Report Optimization
ANALYSIS_GROUP_BY_SEVERITY=true
ANALYSIS_SHOW_STATS=true
ANALYSIS_INCLUDE_RECOMMENDATIONS=true

# SQL Patch Generation
ANALYSIS_GENERATE_BACKUPS=true
ANALYSIS_INCLUDE_ROLLBACKS=true
ANALYSIS_MAX_PATCH_SIZE_MB=10

# Optional: AI Integration (for future use)
GEMINI_API_KEY=your_gemini_api_key_here
'''
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(template)
    
    logger.info(f"已創建配置檔案範本: {filepath}")


if __name__ == "__main__":
    # 創建配置範本
    create_config_template()
    
    # 測試配置載入
    config = get_config()
    print(f"資料庫配置: {config.db_config}")
    print(f"分析配置: {config.analysis_config}")
