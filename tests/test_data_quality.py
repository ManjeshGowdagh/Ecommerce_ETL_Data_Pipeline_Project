"""
Unit tests for data quality validation functions.
"""
import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.data_quality import DataQualityChecker


class TestDataQualityChecker(unittest.TestCase):
    """Test cases for DataQualityChecker class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock()
        self.quality_checker = DataQualityChecker(self.mock_spark)
    
    def test_null_percentage_calculation(self):
        """Test null percentage calculation logic."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.columns = ["col1", "col2"]
        
        # Mock null count query result
        mock_result = Mock()
        mock_result.collect.return_value = [{"null_count": 10}]
        mock_df.select.return_value = mock_result
        
        # Test
        result = self.quality_checker.check_null_percentage(mock_df, "test_table")
        
        # Assertions
        self.assertIsInstance(result, dict)
        self.assertIn("col1", result)
        self.assertIn("col2", result)
    
    def test_duplicate_percentage_calculation(self):
        """Test duplicate percentage calculation."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        mock_deduplicated = Mock()
        mock_deduplicated.count.return_value = 95
        mock_df.dropDuplicates.return_value = mock_deduplicated
        
        # Test
        result = self.quality_checker.check_duplicate_percentage(
            mock_df, ["key_col"], "test_table"
        )
        
        # Assertions
        self.assertEqual(result, 5.0)  # (100-95)/100 * 100 = 5%
    
    def test_business_rule_validation(self):
        """Test business rule validation."""
        from utils.helpers import validate_business_rules
        
        # Valid data
        valid_data = {
            "price": 10.99,
            "quantity": 5,
            "email": "test@example.com"
        }
        violations = validate_business_rules(valid_data)
        self.assertEqual(len(violations), 0)
        
        # Invalid data
        invalid_data = {
            "price": -10.99,
            "quantity": 0,
            "email": "invalid-email"
        }
        violations = validate_business_rules(invalid_data)
        self.assertGreater(len(violations), 0)
    
    @patch('os.makedirs')
    @patch('builtins.open')
    def test_report_generation(self, mock_open, mock_makedirs):
        """Test quality report generation."""
        # Mock report data
        mock_report = {
            "timestamp": "2024-01-01T00:00:00",
            "total_datasets": 1,
            "tables": {
                "test_table": {
                    "record_count": 100,
                    "column_count": 5,
                    "null_percentages": {"col1": 2.5, "col2": 1.0}
                }
            }
        }
        
        # Test CSV report generation
        self.quality_checker.save_quality_report_csv(mock_report, "test_report")
        mock_open.assert_called()
        mock_makedirs.assert_called()
    
    def test_performance_timer(self):
        """Test performance timing utility."""
        from utils.helpers import PerformanceTimer
        
        with PerformanceTimer("test_operation") as timer:
            # Simulate some work
            pass
        
        # Should complete without errors
        self.assertIsNotNone(timer.start_time)


class TestDataIntegrity(unittest.TestCase):
    """Test cases for data integrity validation."""
    
    def test_schema_validation(self):
        """Test schema validation logic."""
        expected_columns = ["order_id", "customer_id", "product_id"]
        actual_columns = ["order_id", "customer_id", "product_id", "quantity"]
        
        missing = set(expected_columns) - set(actual_columns)
        extra = set(actual_columns) - set(expected_columns)
        
        self.assertEqual(len(missing), 0)
        self.assertEqual(list(extra), ["quantity"])
    
    def test_date_format_validation(self):
        """Test date format standardization."""
        import re
        
        # Test valid date formats
        valid_dates = ["2024-01-15", "01/15/2024", "01-15-2024"]
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}'
        ]
        
        for date_str in valid_dates:
            match_found = any(re.match(pattern, date_str) for pattern in date_patterns)
            self.assertTrue(match_found, f"Date {date_str} should match a valid pattern")


if __name__ == "__main__":
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityChecker))
    suite.addTests(loader.loadTestsFromTestCase(TestDataIntegrity))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with error code if tests failed
    sys.exit(0 if result.wasSuccessful() else 1)