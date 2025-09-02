"""
Test runner script for the ETL pipeline.
Executes all test suites and generates coverage reports.
"""
import os
import sys
import unittest
import subprocess
from pathlib import Path


def discover_and_run_tests():
    """Discover and run all tests in the tests directory."""
    # Add src to Python path
    src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
    sys.path.insert(0, src_path)
    
    # Discover tests
    test_dir = os.path.join(os.path.dirname(__file__), '..', 'tests')
    loader = unittest.TestLoader()
    suite = loader.discover(test_dir, pattern='test_*.py')
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    return result


def generate_test_report(result):
    """Generate a test execution report."""
    report_content = f"""# Test Execution Report

**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Tests Run**: {result.testsRun}
- **Failures**: {len(result.failures)}
- **Errors**: {len(result.errors)}
- **Success Rate**: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%

## Test Results
"""
    
    if result.failures:
        report_content += "\n### Failures\n"
        for test, traceback in result.failures:
            report_content += f"- **{test}**\n```\n{traceback}\n```\n"
    
    if result.errors:
        report_content += "\n### Errors\n"
        for test, traceback in result.errors:
            report_content += f"- **{test}**\n```\n{traceback}\n```\n"
    
    if not result.failures and not result.errors:
        report_content += "\n✅ All tests passed successfully!\n"
    
    # Save report
    os.makedirs("reports", exist_ok=True)
    with open("reports/test_report.md", 'w') as f:
        f.write(report_content)
    
    print(f"Test report saved to: reports/test_report.md")


def run_linting():
    """Run code quality checks."""
    print("\n" + "="*50)
    print("Running code quality checks...")
    print("="*50)
    
    # Check if flake8 is available
    try:
        result = subprocess.run(['flake8', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("Running flake8 linting...")
            lint_result = subprocess.run(['flake8', 'src/', '--max-line-length=100'], 
                                       capture_output=True, text=True)
            if lint_result.returncode == 0:
                print("✅ No linting issues found")
            else:
                print("⚠️  Linting issues found:")
                print(lint_result.stdout)
        else:
            print("⚠️  flake8 not available. Install with: pip install flake8")
    except FileNotFoundError:
        print("⚠️  flake8 not found. Install with: pip install flake8")


def main():
    """Main test execution."""
    from datetime import datetime
    
    print("🧪 Running ETL Pipeline Test Suite")
    print("="*50)
    
    # Run tests
    result = discover_and_run_tests()
    
    # Generate report
    generate_test_report(result)
    
    # Run linting
    run_linting()
    
    # Summary
    print("\n" + "="*50)
    print("TEST EXECUTION SUMMARY")
    print("="*50)
    print(f"Tests Run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("✅ All tests passed!")
        exit_code = 0
    else:
        print("❌ Some tests failed!")
        exit_code = 1
    
    print("="*50)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()