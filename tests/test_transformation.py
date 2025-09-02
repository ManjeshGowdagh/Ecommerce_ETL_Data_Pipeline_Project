"""
Unit tests for data transformation functions.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.transformation import DataTransformation


class TestDataTransformation(unittest.TestCase):
    """Test cases for DataTransformation class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock()
        self.transformer = DataTransformation(self.mock_spark)
    
    def test_remove_duplicates(self):
        """Test duplicate removal functionality."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        mock_deduplicated = Mock()
        mock_deduplicated.count.return_value = 95
        mock_df.dropDuplicates.return_value = mock_deduplicated
        
        # Test
        result = self.transformer.remove_duplicates(mock_df, ["key_col"])
        
        # Assertions
        mock_df.dropDuplicates.assert_called_once_with(["key_col"])
        self.assertEqual(result, mock_deduplicated)
    
    def test_handle_null_values_drop_strategy(self):
        """Test null value handling with drop strategy."""
        # Mock DataFrame
        mock_df = Mock()
        mock_filtered = Mock()
        mock_df.filter.return_value = mock_filtered
        
        # Test drop strategy
        strategy_map = {"test_col": "drop"}
        result = self.transformer.handle_null_values(mock_df, strategy_map)
        
        # Assertions
        mock_df.filter.assert_called()
        self.assertEqual(result, mock_filtered)
    
    def test_handle_null_values_fill_strategy(self):
        """Test null value handling with fill strategies."""
        # Mock DataFrame
        mock_df = Mock()
        mock_filled = Mock()
        mock_df.fillna.return_value = mock_filled
        
        # Test fill strategies
        strategy_map = {
            "numeric_col": "zero",
            "text_col": "empty_string",
            "custom_col": "default_value"
        }
        
        result = self.transformer.handle_null_values(mock_df, strategy_map)
        
        # Assertions
        self.assertEqual(mock_df.fillna.call_count, 3)
    
    def test_create_calculated_columns(self):
        """Test calculated column creation."""
        # Mock DataFrame
        mock_df = Mock()
        mock_with_column = Mock()
        mock_df.withColumn.return_value = mock_with_column
        
        # Test
        result = self.transformer.create_calculated_columns(mock_df)
        
        # Assertions
        mock_df.withColumn.assert_called_once()
        self.assertEqual(result, mock_with_column)
    
    def test_validate_business_rules(self):
        """Test business rule validation."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        mock_filtered = Mock()
        mock_filtered.count.return_value = 95
        mock_df.filter.return_value = mock_filtered
        
        # Test
        result = self.transformer.validate_business_rules(mock_df)
        
        # Assertions
        mock_df.filter.assert_called_once()
        self.assertEqual(result, mock_filtered)
    
    @patch('os.makedirs')
    def test_save_transformed_data(self, mock_makedirs):
        """Test saving transformed data."""
        # Mock DataFrame and writer
        mock_df = Mock()
        mock_writer = Mock()
        mock_df.write = mock_writer
        
        # Configure writer chain
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.parquet.return_value = None
        
        # Test
        self.transformer.save_transformed_data(mock_df, "test_table")
        
        # Assertions
        mock_writer.mode.assert_called_with("overwrite")
        mock_writer.option.assert_called_with("compression", "snappy")
        mock_writer.parquet.assert_called()


class TestDataValidation(unittest.TestCase):
    """Test cases for data validation functions."""
    
    def test_date_format_validation(self):
        """Test date format validation logic."""
        import re
        
        # Test date patterns
        valid_dates = [
            "2024-01-15",
            "01/15/2024", 
            "01-15-2024"
        ]
        
        patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{2}-\d{2}-\d{4}'
        ]
        
        for date_str in valid_dates:
            match_found = any(re.match(pattern, date_str) for pattern in patterns)
            self.assertTrue(match_found, f"Date {date_str} should match a pattern")
    
    def test_business_rule_validation(self):
        """Test business rule validation logic."""
        from utils.helpers import validate_business_rules
        
        # Test valid data
        valid_data = {
            "price": 25.99,
            "quantity": 2,
            "email": "customer@example.com"
        }
        violations = validate_business_rules(valid_data)
        self.assertEqual(len(violations), 0)
        
        # Test invalid data
        invalid_data = {
            "price": -10.00,
            "quantity": 0,
            "email": "invalid-email"
        }
        violations = validate_business_rules(invalid_data)
        self.assertGreater(len(violations), 0)
        self.assertIn("Price must be positive", violations)
        self.assertIn("Quantity must be positive", violations)
        self.assertIn("Invalid email format", violations)


class TestPerformanceOptimization(unittest.TestCase):
    """Test cases for performance optimization features."""
    
    def test_performance_timer(self):
        """Test performance timing functionality."""
        from utils.helpers import PerformanceTimer
        import time
        
        with PerformanceTimer("test_operation") as timer:
            time.sleep(0.1)  # Simulate work
        
        self.assertIsNotNone(timer.start_time)
    
    def test_file_size_calculation(self):
        """Test file size calculation utility."""
        from utils.helpers import get_file_size_mb
        
        # Test with non-existent file
        size = get_file_size_mb("non_existent_file.txt")
        self.assertEqual(size, 0.0)
    
    def test_number_formatting(self):
        """Test number formatting utilities."""
        from utils.helpers import format_number_with_commas, percentage_change
        
        # Test comma formatting
        formatted = format_number_with_commas(1234567)
        self.assertEqual(formatted, "1,234,567")
        
        # Test percentage change
        change = percentage_change(100, 120)
        self.assertEqual(change, 20.0)
        
        change_zero_base = percentage_change(0, 50)
        self.assertEqual(change_zero_base, 100.0)


if __name__ == "__main__":
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestDataTransformation))
    suite.addTests(loader.loadTestsFromTestCase(TestDataValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestPerformanceOptimization))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\nTests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    # Exit with error code if tests failed
    sys.exit(0 if result.wasSuccessful() else 1)