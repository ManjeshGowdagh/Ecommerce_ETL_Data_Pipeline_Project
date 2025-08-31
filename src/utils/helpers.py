"""
Utility functions for the ETL pipeline.
Common helper functions used across different modules.
"""
import os
import logging
from typing import Any, Dict, List
from datetime import datetime, timedelta


def setup_logging(log_name: str, log_level: str = "INFO") -> logging.Logger:
    """Set up logging configuration."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(log_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # File handler
    file_handler = logging.FileHandler(f"{log_dir}/{log_name}.log")
    file_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def ensure_directory_exists(directory_path: str) -> None:
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Created directory: {directory_path}")


def get_file_size_mb(file_path: str) -> float:
    """Get file size in megabytes."""
    if os.path.exists(file_path):
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    return 0.0


def validate_file_exists(file_path: str) -> bool:
    """Check if file exists and is accessible."""
    return os.path.exists(file_path) and os.path.isfile(file_path)


def get_processing_timestamp() -> str:
    """Get current timestamp for processing metadata."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def calculate_processing_time(start_time: datetime, end_time: datetime) -> str:
    """Calculate and format processing time."""
    duration = end_time - start_time
    hours, remainder = divmod(duration.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes > 0:
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        return f"{int(seconds)}s"


def format_number_with_commas(number: int) -> str:
    """Format large numbers with commas for readability."""
    return f"{number:,}"


def percentage_change(old_value: float, new_value: float) -> float:
    """Calculate percentage change between two values."""
    if old_value == 0:
        return 0.0 if new_value == 0 else 100.0
    return round(((new_value - old_value) / old_value) * 100, 2)


def create_sample_data_summary(datasets: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Create a summary of dataset statistics."""
    summary = {}
    
    for name, df in datasets.items():
        try:
            record_count = df.count() if hasattr(df, 'count') else len(df)
            column_count = len(df.columns) if hasattr(df, 'columns') else 0
            
            summary[name] = {
                "record_count": record_count,
                "column_count": column_count,
                "size_estimate_mb": record_count * column_count * 0.001  # Rough estimate
            }
        except Exception as e:
            summary[name] = {"error": str(e)}
    
    return summary


def validate_business_rules(data: Dict[str, Any]) -> List[str]:
    """Validate business rules against the data."""
    violations = []
    
    # Example business rules
    if 'price' in data and data['price'] <= 0:
        violations.append("Price must be positive")
    
    if 'quantity' in data and data['quantity'] <= 0:
        violations.append("Quantity must be positive")
    
    if 'email' in data and data['email'] and '@' not in data['email']:
        violations.append("Invalid email format")
    
    return violations


class PerformanceTimer:
    """Context manager for measuring execution time."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.operation_name}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = calculate_processing_time(self.start_time, end_time)
        
        if exc_type is None:
            self.logger.info(f"Completed {self.operation_name} in {duration}")
        else:
            self.logger.error(f"Failed {self.operation_name} after {duration}: {exc_val}")