"""
Configuration settings for the ETL pipeline.
"""
import os
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class PipelineConfig:
    """Configuration class for ETL pipeline settings."""
    
    # File paths
    raw_data_path: str = "data/raw"
    processed_data_path: str = "data/processed"
    reports_path: str = "reports"
    logs_path: str = "logs"
    
    # Spark configuration
    spark_app_name: str = "EcommerceETL"
    spark_master: str = "local[*]"
    spark_sql_warehouse_dir: str = "spark-warehouse"
    
    # Data quality thresholds
    max_null_percentage: float = 5.0
    max_duplicate_percentage: float = 1.0
    min_data_freshness_hours: int = 24
    
    # File formats
    output_format: str = "parquet"
    compression: str = "snappy"
    
    # Database settings
    hive_database: str = "ecommerce_dw"
    
    # Partitioning
    partition_columns: List[str] = None
    
    def __post_init__(self):
        if self.partition_columns is None:
            self.partition_columns = ["year", "month"]
    
    @property
    def quality_thresholds(self) -> Dict[str, float]:
        """Return data quality thresholds as a dictionary."""
        return {
            "null_percentage": self.max_null_percentage,
            "duplicate_percentage": self.max_duplicate_percentage,
            "data_freshness_hours": self.min_data_freshness_hours
        }


# Environment-specific configurations
def get_config(environment: str = "development") -> PipelineConfig:
    """Get configuration based on environment."""
    
    base_config = PipelineConfig()
    
    if environment == "production":
        base_config.spark_master = "yarn"
        base_config.raw_data_path = "hdfs://namenode:9000/data/raw"
        base_config.processed_data_path = "hdfs://namenode:9000/data/processed"
        base_config.max_null_percentage = 2.0
        
    elif environment == "staging":
        base_config.spark_master = "yarn"
        base_config.max_null_percentage = 3.0
        
    return base_config


# Global configuration instance
CONFIG = get_config(os.getenv("ENVIRONMENT", "development"))