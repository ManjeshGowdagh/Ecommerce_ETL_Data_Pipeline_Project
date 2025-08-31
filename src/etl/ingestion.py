"""
Data ingestion module for the e-commerce ETL pipeline.
Handles reading raw CSV files and basic validation.
"""
import os
import logging
from typing import Dict, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from config.pipeline_config import CONFIG


class DataIngestion:
    """Handles ingestion of raw e-commerce data files."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        
    def get_orders_schema(self) -> StructType:
        """Define schema for orders data."""
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("order_date", StringType(), True),
            StructField("payment_type", StringType(), True)
        ])
    
    def get_customers_schema(self) -> StructType:
        """Define schema for customers data."""
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("registration_date", StringType(), True)
        ])
    
    def get_products_schema(self) -> StructType:
        """Define schema for products data."""
        return StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("cost", DoubleType(), True),
            StructField("description", StringType(), True)
        ])
    
    def get_payments_schema(self) -> StructType:
        """Define schema for payments data."""
        return StructType([
            StructField("payment_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("payment_method", StringType(), True),
            StructField("payment_amount", DoubleType(), True),
            StructField("payment_date", StringType(), True),
            StructField("payment_status", StringType(), True)
        ])
    
    def read_csv_file(self, file_path: str, schema: StructType) -> Optional[DataFrame]:
        """Read CSV file with specified schema."""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File not found: {file_path}")
                return None
                
            df = self.spark.read.csv(
                file_path,
                header=True,
                schema=schema,
                timestampFormat="yyyy-MM-dd HH:mm:ss"
            )
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} records from {file_path}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            return None
    
    def ingest_all_data(self) -> Dict[str, DataFrame]:
        """Ingest all raw data files."""
        datasets = {}
        
        # Define file mappings
        file_configs = {
            "orders": (f"{CONFIG.raw_data_path}/orders.csv", self.get_orders_schema()),
            "customers": (f"{CONFIG.raw_data_path}/customers.csv", self.get_customers_schema()),
            "products": (f"{CONFIG.raw_data_path}/products.csv", self.get_products_schema()),
            "payments": (f"{CONFIG.raw_data_path}/payments.csv", self.get_payments_schema())
        }
        
        for dataset_name, (file_path, schema) in file_configs.items():
            df = self.read_csv_file(file_path, schema)
            if df is not None:
                datasets[dataset_name] = df
                self.logger.info(f"Ingested {dataset_name} dataset")
            else:
                self.logger.warning(f"Failed to ingest {dataset_name} dataset")
        
        return datasets
    
    def validate_ingested_data(self, datasets: Dict[str, DataFrame]) -> bool:
        """Perform basic validation on ingested data."""
        validation_passed = True
        
        for name, df in datasets.items():
            try:
                # Check if dataframe is not empty
                if df.count() == 0:
                    self.logger.error(f"{name} dataset is empty")
                    validation_passed = False
                    continue
                
                # Check for required columns based on schema
                expected_columns = [field.name for field in df.schema.fields]
                actual_columns = df.columns
                
                missing_columns = set(expected_columns) - set(actual_columns)
                if missing_columns:
                    self.logger.error(f"{name} missing columns: {missing_columns}")
                    validation_passed = False
                
                self.logger.info(f"{name} validation passed")
                
            except Exception as e:
                self.logger.error(f"Validation error for {name}: {str(e)}")
                validation_passed = False
        
        return validation_passed


def main():
    """Main execution function."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{CONFIG.logs_path}/ingestion.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(CONFIG.spark_app_name) \
        .master(CONFIG.spark_master) \
        .config("spark.sql.warehouse.dir", CONFIG.spark_sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Initialize ingestion
        ingestion = DataIngestion(spark)
        
        # Ingest all datasets
        datasets = ingestion.ingest_all_data()
        
        # Validate ingested data
        if ingestion.validate_ingested_data(datasets):
            logging.info("Data ingestion completed successfully")
        else:
            logging.error("Data ingestion validation failed")
            
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()