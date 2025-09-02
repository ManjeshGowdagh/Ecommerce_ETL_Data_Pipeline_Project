"""
Pipeline validation script.
Validates the entire ETL pipeline end-to-end.
"""
import os
import sys
import logging
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from config.pipeline_config import CONFIG
from utils.helpers import setup_logging, ensure_directory_exists


def validate_file_structure():
    """Validate that all required files and directories exist."""
    print("🔍 Validating project structure...")
    
    required_files = [
        "src/etl/ingestion.py",
        "src/etl/transformation.py", 
        "src/etl/data_quality.py",
        "src/sql/create_tables.sql",
        "src/sql/star_schema.sql",
        "config/pipeline_config.py",
        "run_pipeline.py"
    ]
    
    required_dirs = [
        "data/raw",
        "data/processed", 
        "reports",
        "logs",
        "tests"
    ]
    
    missing_files = []
    missing_dirs = []
    
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
        else:
            print(f"✓ {file_path}")
    
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            missing_dirs.append(dir_path)
        else:
            print(f"✓ {dir_path}/")
    
    if missing_files or missing_dirs:
        print("\n❌ Missing required files/directories:")
        for item in missing_files + missing_dirs:
            print(f"   - {item}")
        return False
    
    print("✅ Project structure validation passed")
    return True


def validate_sample_data():
    """Validate that sample data files exist and are properly formatted."""
    print("\n🔍 Validating sample data...")
    
    data_files = ["orders.csv", "customers.csv", "products.csv", "payments.csv"]
    
    for filename in data_files:
        filepath = f"data/raw/{filename}"
        
        if not os.path.exists(filepath):
            print(f"❌ Missing data file: {filepath}")
            return False
        
        # Check file is not empty
        if os.path.getsize(filepath) == 0:
            print(f"❌ Empty data file: {filepath}")
            return False
        
        # Basic CSV validation
        try:
            with open(filepath, 'r') as f:
                first_line = f.readline().strip()
                if not first_line:
                    print(f"❌ Invalid CSV header in: {filepath}")
                    return False
                
                # Count lines
                line_count = sum(1 for line in f) + 1  # +1 for header
                print(f"✓ {filename}: {line_count} lines")
                
        except Exception as e:
            print(f"❌ Error reading {filepath}: {str(e)}")
            return False
    
    print("✅ Sample data validation passed")
    return True


def validate_configuration():
    """Validate pipeline configuration."""
    print("\n🔍 Validating configuration...")
    
    try:
        # Test configuration import
        from config.pipeline_config import CONFIG
        
        # Check required attributes
        required_attrs = [
            "raw_data_path", "processed_data_path", "reports_path", "logs_path",
            "spark_app_name", "spark_master", "max_null_percentage", "max_duplicate_percentage"
        ]
        
        for attr in required_attrs:
            if not hasattr(CONFIG, attr):
                print(f"❌ Missing configuration attribute: {attr}")
                return False
            print(f"✓ {attr}: {getattr(CONFIG, attr)}")
        
        print("✅ Configuration validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {str(e)}")
        return False


def validate_python_imports():
    """Validate that all required Python modules can be imported."""
    print("\n🔍 Validating Python imports...")
    
    required_modules = [
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("pyspark", "pyspark.sql"),
        ("logging", "logging"),
        ("datetime", "datetime"),
        ("os", "os"),
        ("sys", "sys")
    ]
    
    for module_name, import_path in required_modules:
        try:
            __import__(import_path)
            print(f"✓ {module_name}")
        except ImportError as e:
            print(f"❌ Failed to import {module_name}: {str(e)}")
            return False
    
    print("✅ Python imports validation passed")
    return True


def validate_etl_modules():
    """Validate that ETL modules can be imported and initialized."""
    print("\n🔍 Validating ETL modules...")
    
    try:
        # Test ingestion module
        from etl.ingestion import DataIngestion
        print("✓ DataIngestion module")
        
        # Test transformation module
        from etl.transformation import DataTransformation
        print("✓ DataTransformation module")
        
        # Test data quality module
        from etl.data_quality import DataQualityChecker
        print("✓ DataQualityChecker module")
        
        # Test utilities
        from utils.helpers import setup_logging, PerformanceTimer
        print("✓ Helper utilities")
        
        print("✅ ETL modules validation passed")
        return True
        
    except Exception as e:
        print(f"❌ ETL modules validation failed: {str(e)}")
        return False


def run_basic_functionality_test():
    """Run basic functionality test to ensure pipeline components work."""
    print("\n🔍 Running basic functionality test...")
    
    try:
        # Test logging setup
        logger = setup_logging("validation_test", "INFO")
        logger.info("Test log message")
        print("✓ Logging functionality")
        
        # Test performance timer
        from utils.helpers import PerformanceTimer
        with PerformanceTimer("test_operation"):
            import time
            time.sleep(0.1)
        print("✓ Performance timer")
        
        # Test directory creation
        test_dir = "tmp/validation_test"
        ensure_directory_exists(test_dir)
        if os.path.exists(test_dir):
            os.rmdir(test_dir)
            print("✓ Directory utilities")
        
        print("✅ Basic functionality test passed")
        return True
        
    except Exception as e:
        print(f"❌ Basic functionality test failed: {str(e)}")
        return False


def generate_validation_report(results: Dict[str, bool]):
    """Generate validation report."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"reports/validation_report_{timestamp}.md"
    
    os.makedirs("reports", exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write("# Pipeline Validation Report\n\n")
        f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Validation Results\n\n")
        
        all_passed = True
        for test_name, passed in results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            f.write(f"- **{test_name}**: {status}\n")
            if not passed:
                all_passed = False
        
        f.write(f"\n## Overall Status: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}\n")
        
        if not all_passed:
            f.write("\n### Next Steps\n")
            f.write("1. Review failed validation checks above\n")
            f.write("2. Fix any missing dependencies or configuration issues\n")
            f.write("3. Re-run validation: `python scripts/validate_pipeline.py`\n")
        else:
            f.write("\n### Ready for Execution\n")
            f.write("The pipeline is ready to run:\n")
            f.write("```bash\n")
            f.write("python run_pipeline.py\n")
            f.write("```\n")
    
    print(f"\n📄 Validation report saved to: {report_path}")


def main():
    """Main validation execution."""
    print("🔧 ETL Pipeline Validation")
    print("="*50)
    
    # Run validation tests
    validation_tests = [
        ("File Structure", validate_file_structure),
        ("Sample Data", validate_sample_data),
        ("Configuration", validate_configuration),
        ("Python Imports", validate_python_imports),
        ("ETL Modules", validate_etl_modules),
        ("Basic Functionality", run_basic_functionality_test)
    ]
    
    results = {}
    
    for test_name, test_function in validation_tests:
        try:
            results[test_name] = test_function()
        except Exception as e:
            print(f"❌ {test_name} validation failed with exception: {str(e)}")
            results[test_name] = False
    
    # Generate report
    generate_validation_report(results)
    
    # Summary
    passed_tests = sum(results.values())
    total_tests = len(results)
    
    print("\n" + "="*50)
    print("VALIDATION SUMMARY")
    print("="*50)
    print(f"Tests Passed: {passed_tests}/{total_tests}")
    
    if passed_tests == total_tests:
        print("✅ All validations passed! Pipeline is ready to run.")
        exit_code = 0
    else:
        print("❌ Some validations failed. Please review and fix issues.")
        exit_code = 1
    
    print("="*50)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()