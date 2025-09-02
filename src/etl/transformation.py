"""
Data transformation module using PySpark.
Handles data cleaning, validation, and preparation for star schema.
"""
import logging
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, to_date, 
    year, month, dayofmonth, dayofweek, date_format,
    mean, stddev, max as spark_max, min as spark_min,
    monotonically_increasing_id, desc, asc
)
from pyspark.sql.types import DoubleType, IntegerType

from config.pipeline_config import CONFIG


class DataTransformation:
    """Handles data cleaning and transformation operations."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
    
    def remove_duplicates(self, df: DataFrame, key_columns: List[str]) -> DataFrame:
        """Remove duplicate records based on key columns."""
        initial_count = df.count()
        
        # Remove duplicates keeping the most recent record
        df_deduplicated = df.dropDuplicates(key_columns)
        
        final_count = df_deduplicated.count()
        removed_count = initial_count - final_count
        
        self.logger.info(f"Removed {removed_count} duplicate records")
        
        return df_deduplicated
    
    def handle_null_values(self, df: DataFrame, strategy_map: Dict[str, str]) -> DataFrame:
        """Handle null values based on specified strategies."""
        df_cleaned = df
        
        for column, strategy in strategy_map.items():
            if column not in df.columns:
                continue
                
            if strategy == "drop":
                df_cleaned = df_cleaned.filter(col(column).isNotNull())
                
            elif strategy == "median":
                if df.schema[column].dataType in [DoubleType(), IntegerType()]:
                    median_value = df.select(column).rdd.map(lambda x: x[0]).filter(lambda x: x is not None).median()
                    df_cleaned = df_cleaned.fillna({column: median_value})
                    
            elif strategy == "mean":
                if df.schema[column].dataType in [DoubleType(), IntegerType()]:
                    mean_value = df.agg(mean(col(column))).collect()[0][0]
                    df_cleaned = df_cleaned.fillna({column: mean_value})
                    
            elif strategy == "zero":
                df_cleaned = df_cleaned.fillna({column: 0})
                
            elif strategy == "empty_string":
                df_cleaned = df_cleaned.fillna({column: ""})
                
            elif isinstance(strategy, (str, int, float)):
                df_cleaned = df_cleaned.fillna({column: strategy})
        
        return df_cleaned
    
    def standardize_dates(self, df: DataFrame, date_columns: List[str]) -> DataFrame:
        """Standardize date columns to yyyy-MM-dd format."""
        df_standardized = df
        
        for date_col in date_columns:
            if date_col in df.columns:
                # Handle multiple date formats
                df_standardized = df_standardized.withColumn(
                    date_col,
                    when(col(date_col).rlike(r'\d{4}-\d{2}-\d{2}'), to_date(col(date_col), "yyyy-MM-dd"))
                    .when(col(date_col).rlike(r'\d{2}/\d{2}/\d{4}'), to_date(col(date_col), "MM/dd/yyyy"))
                    .when(col(date_col).rlike(r'\d{2}-\d{2}-\d{4}'), to_date(col(date_col), "MM-dd-yyyy"))
                    .otherwise(None)
                )
                
        return df_standardized
    
    def create_calculated_columns(self, df: DataFrame) -> DataFrame:
        """Create calculated columns for business logic."""
        return df.withColumn(
            "total_amount",
            col("quantity") * col("unit_price")
        )
    
    def validate_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rule validations."""
        # Filter out invalid records
        df_valid = df.filter(
            (col("quantity") > 0) &
            (col("unit_price") > 0) &
            (col("total_amount") > 0)
        )
        
        invalid_count = df.count() - df_valid.count()
        if invalid_count > 0:
            self.logger.warning(f"Filtered out {invalid_count} records due to business rule violations")
        
        return df_valid
    
    def transform_orders(self, orders_df: DataFrame) -> DataFrame:
        """Transform orders dataset."""
        self.logger.info("Transforming orders data...")
        
        # Remove duplicates
        orders_clean = self.remove_duplicates(orders_df, ["order_id"])
        
        # Handle null values
        null_strategy = {
            "quantity": "drop",
            "unit_price": "median",
            "payment_type": "Unknown"
        }
        orders_clean = self.handle_null_values(orders_clean, null_strategy)
        
        # Standardize dates
        orders_clean = self.standardize_dates(orders_clean, ["order_date"])
        
        # Create calculated columns
        orders_clean = self.create_calculated_columns(orders_clean)
        
        # Apply business rules
        orders_clean = self.validate_business_rules(orders_clean)
        
        self.logger.info(f"Orders transformation completed. Final record count: {orders_clean.count()}")
        
        return orders_clean
    
    def transform_customers(self, customers_df: DataFrame) -> DataFrame:
        """Transform customers dataset."""
        self.logger.info("Transforming customers data...")
        
        # Remove duplicates
        customers_clean = self.remove_duplicates(customers_df, ["customer_id"])
        
        # Handle null values
        null_strategy = {
            "customer_name": "drop",
            "email": "drop",
            "phone": "Unknown",
            "address": "Unknown",
            "city": "Unknown",
            "state": "Unknown",
            "zip_code": "00000",
            "age": "median"
        }
        customers_clean = self.handle_null_values(customers_clean, null_strategy)
        
        # Standardize dates
        customers_clean = self.standardize_dates(customers_clean, ["registration_date"])
        
        # Create age groups
        customers_clean = customers_clean.withColumn(
            "age_group",
            when(col("age") < 25, "18-24")
            .when(col("age") < 35, "25-34")
            .when(col("age") < 45, "35-44")
            .when(col("age") < 55, "45-54")
            .when(col("age") < 65, "55-64")
            .otherwise("65+")
        )
        
        self.logger.info(f"Customers transformation completed. Final record count: {customers_clean.count()}")
        
        return customers_clean
    
    def transform_products(self, products_df: DataFrame) -> DataFrame:
        """Transform products dataset."""
        self.logger.info("Transforming products data...")
        
        # Remove duplicates
        products_clean = self.remove_duplicates(products_df, ["product_id"])
        
        # Handle null values
        null_strategy = {
            "product_name": "drop",
            "category": "Unknown",
            "subcategory": "Unknown",
            "brand": "Unknown",
            "price": "median",
            "cost": "median",
            "description": "No description available"
        }
        products_clean = self.handle_null_values(products_clean, null_strategy)
        
        # Create price categories
        products_clean = products_clean.withColumn(
            "price_category",
            when(col("price") < 25, "Budget")
            .when(col("price") < 100, "Standard")
            .when(col("price") < 500, "Premium")
            .otherwise("Luxury")
        )
        
        # Calculate profit margin
        products_clean = products_clean.withColumn(
            "profit_margin",
            ((col("price") - col("cost")) / col("price") * 100).cast(DoubleType())
        )
        
        self.logger.info(f"Products transformation completed. Final record count: {products_clean.count()}")
        
        return products_clean
    
    def transform_payments(self, payments_df: DataFrame) -> DataFrame:
        """Transform payments dataset."""
        self.logger.info("Transforming payments data...")
        
        # Remove duplicates
        payments_clean = self.remove_duplicates(payments_df, ["payment_id"])
        
        # Handle null values
        null_strategy = {
            "payment_method": "Unknown",
            "payment_amount": "drop",
            "payment_status": "Unknown"
        }
        payments_clean = self.handle_null_values(payments_clean, null_strategy)
        
        # Standardize dates
        payments_clean = self.standardize_dates(payments_clean, ["payment_date"])
        
        # Standardize payment methods
        payments_clean = payments_clean.withColumn(
            "payment_method",
            when(col("payment_method").isin(["cc", "credit_card", "creditcard"]), "Credit Card")
            .when(col("payment_method").isin(["dc", "debit_card", "debitcard"]), "Debit Card")
            .when(col("payment_method").isin(["paypal", "pp"]), "PayPal")
            .when(col("payment_method").isin(["cash", "cash_on_delivery", "cod"]), "Cash")
            .otherwise(col("payment_method"))
        )
        
        self.logger.info(f"Payments transformation completed. Final record count: {payments_clean.count()}")
        
        return payments_clean
    
    def save_transformed_data(self, df: DataFrame, table_name: str) -> None:
        """Save transformed data to Parquet format."""
        output_path = f"{CONFIG.processed_data_path}/{table_name}"
        
        try:
            df.write \
                .mode("overwrite") \
                .option("compression", CONFIG.compression) \
                .parquet(output_path)
                
            self.logger.info(f"Saved {table_name} to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving {table_name}: {str(e)}")
            raise


def main():
    """Main transformation pipeline execution."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{CONFIG.logs_path}/transformation.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"{CONFIG.spark_app_name}_Transformation") \
        .master(CONFIG.spark_master) \
        .config("spark.sql.warehouse.dir", CONFIG.spark_sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Initialize transformation
        transformer = DataTransformation(spark)
        
        # Read raw data (assume ingestion has been completed)
        from src.etl.ingestion import DataIngestion
        ingestion = DataIngestion(spark)
        raw_datasets = ingestion.ingest_all_data()
        
        # Transform each dataset
        transformed_datasets = {}
        
        if "orders" in raw_datasets:
            transformed_datasets["orders_clean"] = transformer.transform_orders(raw_datasets["orders"])
            
        if "customers" in raw_datasets:
            transformed_datasets["customers_clean"] = transformer.transform_customers(raw_datasets["customers"])
            
        if "products" in raw_datasets:
            transformed_datasets["products_clean"] = transformer.transform_products(raw_datasets["products"])
            
        if "payments" in raw_datasets:
            transformed_datasets["payments_clean"] = transformer.transform_payments(raw_datasets["payments"])
        
        # Save transformed data
        for table_name, df in transformed_datasets.items():
            transformer.save_transformed_data(df, table_name)
            
        logging.info("Data transformation pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Transformation pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()