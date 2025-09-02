"""
Environment setup script for the ETL pipeline.
Creates necessary directories and validates dependencies.
"""
import os
import sys
import subprocess
import logging
from pathlib import Path


def create_directory_structure():
    """Create the required directory structure."""
    directories = [
        "data/raw",
        "data/processed",
        "data/processed/dashboard",
        "reports",
        "logs",
        "spark-warehouse",
        "tmp"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")


def validate_python_version():
    """Validate Python version requirements."""
    required_version = (3, 8)
    current_version = sys.version_info[:2]
    
    if current_version < required_version:
        print(f"❌ Python {required_version[0]}.{required_version[1]}+ required. Current: {current_version[0]}.{current_version[1]}")
        return False
    
    print(f"✓ Python version: {current_version[0]}.{current_version[1]}")
    return True


def check_dependencies():
    """Check if required dependencies are available."""
    dependencies = [
        "pyspark",
        "pandas", 
        "numpy",
        "pytest"
    ]
    
    missing_deps = []
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✓ {dep} is available")
        except ImportError:
            missing_deps.append(dep)
            print(f"❌ {dep} is missing")
    
    if missing_deps:
        print(f"\nInstall missing dependencies with:")
        print(f"pip install {' '.join(missing_deps)}")
        return False
    
    return True


def setup_spark_environment():
    """Set up Spark environment variables."""
    spark_home = os.getenv("SPARK_HOME")
    
    if not spark_home:
        print("⚠️  SPARK_HOME not set. Using local Spark mode.")
        print("   For production, install Apache Spark and set SPARK_HOME")
    else:
        print(f"✓ SPARK_HOME: {spark_home}")
    
    # Set default Spark configurations
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    
    print("✓ Spark environment configured")


def create_sample_config():
    """Create sample configuration files."""
    config_content = """# ETL Pipeline Configuration

# Environment Settings
ENVIRONMENT=development
LOG_LEVEL=INFO

# Spark Settings
SPARK_MASTER=local[*]
SPARK_APP_NAME=EcommerceETL

# Data Quality Thresholds
MAX_NULL_PERCENTAGE=5.0
MAX_DUPLICATE_PERCENTAGE=1.0

# File Paths
RAW_DATA_PATH=data/raw
PROCESSED_DATA_PATH=data/processed
REPORTS_PATH=reports
LOGS_PATH=logs
"""
    
    config_path = ".env"
    if not os.path.exists(config_path):
        with open(config_path, 'w') as f:
            f.write(config_content)
        print(f"✓ Created sample configuration: {config_path}")
    else:
        print(f"✓ Configuration file exists: {config_path}")


def run_initial_tests():
    """Run basic tests to validate setup."""
    print("\nRunning initial validation tests...")
    
    try:
        # Test Python imports
        import pandas as pd
        import numpy as np
        print("✓ Core Python libraries working")
        
        # Test basic functionality
        test_data = pd.DataFrame({"test": [1, 2, 3]})
        assert len(test_data) == 3
        print("✓ Basic data processing working")
        
        return True
        
    except Exception as e:
        print(f"❌ Initial tests failed: {str(e)}")
        return False


def main():
    """Main setup execution."""
    print("🚀 Setting up E-commerce ETL Pipeline Environment\n")
    
    # Validation steps
    steps = [
        ("Validating Python version", validate_python_version),
        ("Creating directory structure", create_directory_structure),
        ("Checking dependencies", check_dependencies),
        ("Setting up Spark environment", setup_spark_environment),
        ("Creating sample configuration", create_sample_config),
        ("Running initial tests", run_initial_tests)
    ]
    
    failed_steps = []
    
    for step_name, step_function in steps:
        print(f"\n{step_name}...")
        try:
            if callable(step_function):
                success = step_function()
                if success is False:
                    failed_steps.append(step_name)
            else:
                step_function()
        except Exception as e:
            print(f"❌ {step_name} failed: {str(e)}")
            failed_steps.append(step_name)
    
    # Summary
    print("\n" + "="*60)
    if failed_steps:
        print("❌ Setup completed with issues:")
        for step in failed_steps:
            print(f"   - {step}")
        print("\nPlease resolve the issues above before running the pipeline.")
    else:
        print("✅ Environment setup completed successfully!")
        print("\nNext steps:")
        print("1. Run the pipeline: python run_pipeline.py")
        print("2. Check logs in the 'logs' directory")
        print("3. View reports in the 'reports' directory")
    
    print("="*60)


if __name__ == "__main__":
    main()