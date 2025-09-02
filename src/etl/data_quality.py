"""
Data quality validation and reporting module.
Performs comprehensive quality checks and generates reports.
"""
import os
import csv
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull
from pyspark.sql.functions import mean, min as spark_min, max as spark_max, desc</parameter>

from config.pipeline_config import CONFIG


class DataQualityChecker:
    """Performs comprehensive data quality checks and generates reports."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        self.quality_report = {}
    
    def check_null_percentage(self, df: DataFrame, table_name: str) -> Dict[str, float]:
        """Calculate percentage of null values per column."""
        total_records = df.count()
        null_percentages = {}
        
        for column in df.columns:
            null_count = df.select(
                spark_sum(
                    when(col(column).isNull() | isnan(col(column)), 1).otherwise(0)
                ).alias("null_count")
            ).collect()[0]["null_count"]
            
            null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
            null_percentages[column] = round(null_percentage, 2)
            
            if null_percentage > CONFIG.max_null_percentage:
                self.logger.warning(f"{table_name}.{column}: {null_percentage}% null values exceeds threshold")
        
        return null_percentages
    
    def check_duplicate_percentage(self, df: DataFrame, key_columns: List[str], table_name: str) -> float:
        """Calculate percentage of duplicate records."""
        total_records = df.count()
        unique_records = df.dropDuplicates(key_columns).count()
        
        duplicate_count = total_records - unique_records
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        
        if duplicate_percentage > CONFIG.max_duplicate_percentage:
            self.logger.warning(f"{table_name}: {duplicate_percentage}% duplicate records exceeds threshold")
        
        return round(duplicate_percentage, 2)
    
    def check_foreign_key_integrity(self, fact_df: DataFrame, dim_df: DataFrame, 
                                   fk_column: str, pk_column: str, table_names: Tuple[str, str]) -> float:
        """Check foreign key integrity between fact and dimension tables."""
        fact_table, dim_table = table_names
        
        # Get unique foreign keys from fact table
        fact_keys = fact_df.select(fk_column).distinct()
        
        # Left anti join to find orphaned records
        orphaned_records = fact_keys.join(
            dim_df.select(pk_column), 
            fact_keys[fk_column] == dim_df[pk_column], 
            "left_anti"
        )
        
        orphaned_count = orphaned_records.count()
        total_fact_records = fact_df.count()
        
        integrity_percentage = ((total_fact_records - orphaned_count) / total_fact_records) * 100 if total_fact_records > 0 else 100
        
        if orphaned_count > 0:
            self.logger.warning(f"{orphaned_count} orphaned records found in {fact_table}.{fk_column}")
        
        return round(integrity_percentage, 2)
    
    def check_data_distribution(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """Analyze data distribution for a specific column."""
        if df.schema[column].dataType in [IntegerType(), DoubleType()]:
            stats = df.agg(
                spark_min(col(column)).alias("min"),
                spark_max(col(column)).alias("max"),
                mean(col(column)).alias("mean"),
                stddev(col(column)).alias("stddev")
            ).collect()[0]
            
            return {
                "min": stats["min"],
                "max": stats["max"],
                "mean": round(stats["mean"], 2) if stats["mean"] else None,
                "stddev": round(stats["stddev"], 2) if stats["stddev"] else None
            }
        else:
            # For categorical data, get top values
            top_values = df.groupBy(column).count().orderBy(desc("count")).limit(5).collect()
            return {
                "top_values": [(row[column], row["count"]) for row in top_values]
            }
    
    def generate_quality_report(self, datasets: Dict[str, DataFrame]) -> Dict[str, Any]:
        """Generate comprehensive data quality report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_datasets": len(datasets),
            "tables": {}
        }
        
        for table_name, df in datasets.items():
            self.logger.info(f"Running quality checks for {table_name}")
            
            table_report = {
                "record_count": df.count(),
                "column_count": len(df.columns),
                "null_percentages": self.check_null_percentage(df, table_name),
                "data_types": {field.name: str(field.dataType) for field in df.schema.fields}
            }
            
            # Table-specific checks
            if table_name == "orders_clean":
                table_report["duplicate_percentage"] = self.check_duplicate_percentage(
                    df, ["order_id"], table_name
                )
                
            elif table_name == "customers_clean":
                table_report["duplicate_percentage"] = self.check_duplicate_percentage(
                    df, ["customer_id"], table_name
                )
                
            elif table_name == "products_clean":
                table_report["duplicate_percentage"] = self.check_duplicate_percentage(
                    df, ["product_id"], table_name
                )
                table_report["price_distribution"] = self.check_data_distribution(df, "price")
                
            elif table_name == "payments_clean":
                table_report["duplicate_percentage"] = self.check_duplicate_percentage(
                    df, ["payment_id"], table_name
                )
            
            report["tables"][table_name] = table_report
        
        # Cross-table integrity checks
        if "orders_clean" in datasets and "customers_clean" in datasets:
            integrity = self.check_foreign_key_integrity(
                datasets["orders_clean"], datasets["customers_clean"],
                "customer_id", "customer_id", ("orders", "customers")
            )
            report["foreign_key_integrity"] = {
                "orders_customers": integrity
            }
        
        return report
    
    def save_quality_report_csv(self, report: Dict[str, Any], filename: str) -> None:
        """Save quality report as CSV file."""
        csv_path = f"{CONFIG.reports_path}/{filename}.csv"
        os.makedirs(CONFIG.reports_path, exist_ok=True)
        
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Table", "Metric", "Value"])
            
            for table_name, table_data in report["tables"].items():
                writer.writerow([table_name, "Record Count", table_data["record_count"]])
                writer.writerow([table_name, "Column Count", table_data["column_count"]])
                
                if "duplicate_percentage" in table_data:
                    writer.writerow([table_name, "Duplicate Percentage", table_data["duplicate_percentage"]])
                
                for column, null_pct in table_data["null_percentages"].items():
                    writer.writerow([table_name, f"Null % - {column}", null_pct])
        
        self.logger.info(f"Quality report saved to {csv_path}")
    
    def save_quality_report_markdown(self, report: Dict[str, Any], filename: str) -> None:
        """Save quality report as Markdown file."""
        md_path = f"{CONFIG.reports_path}/{filename}.md"
        os.makedirs(CONFIG.reports_path, exist_ok=True)
        
        with open(md_path, 'w') as mdfile:
            mdfile.write(f"# Data Quality Report\n\n")
            mdfile.write(f"**Generated**: {report['timestamp']}\n\n")
            mdfile.write(f"**Total Datasets**: {report['total_datasets']}\n\n")
            
            # Overall summary
            mdfile.write("## Summary\n\n")
            for table_name, table_data in report["tables"].items():
                mdfile.write(f"- **{table_name}**: {table_data['record_count']} records, {table_data['column_count']} columns\n")
            
            mdfile.write("\n## Detailed Quality Metrics\n\n")
            
            for table_name, table_data in report["tables"].items():
                mdfile.write(f"### {table_name}\n\n")
                mdfile.write(f"- **Record Count**: {table_data['record_count']:,}\n")
                mdfile.write(f"- **Column Count**: {table_data['column_count']}\n")
                
                if "duplicate_percentage" in table_data:
                    status = "✅ PASS" if table_data["duplicate_percentage"] <= CONFIG.max_duplicate_percentage else "❌ FAIL"
                    mdfile.write(f"- **Duplicate Percentage**: {table_data['duplicate_percentage']}% {status}\n")
                
                mdfile.write("\n#### Null Value Analysis\n\n")
                mdfile.write("| Column | Null % | Status |\n")
                mdfile.write("|--------|--------|--------|\n")
                
                for column, null_pct in table_data["null_percentages"].items():
                    status = "✅ PASS" if null_pct <= CONFIG.max_null_percentage else "❌ FAIL"
                    mdfile.write(f"| {column} | {null_pct}% | {status} |\n")
                
                mdfile.write("\n")
            
            # Foreign key integrity
            if "foreign_key_integrity" in report:
                mdfile.write("## Foreign Key Integrity\n\n")
                for relationship, integrity in report["foreign_key_integrity"].items():
                    status = "✅ PASS" if integrity > 95 else "❌ FAIL"
                    mdfile.write(f"- **{relationship}**: {integrity}% {status}\n")
        
        self.logger.info(f"Quality report saved to {md_path}")


def main():
    """Main data quality validation execution."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{CONFIG.logs_path}/data_quality.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"{CONFIG.spark_app_name}_DataQuality") \
        .master(CONFIG.spark_master) \
        .config("spark.sql.warehouse.dir", CONFIG.spark_sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Initialize quality checker
        quality_checker = DataQualityChecker(spark)
        
        # Load transformed datasets
        datasets = {}
        processed_path = CONFIG.processed_data_path
        
        for table_name in ["orders_clean", "customers_clean", "products_clean", "payments_clean"]:
            table_path = f"{processed_path}/{table_name}"
            if os.path.exists(table_path):
                datasets[table_name] = spark.read.parquet(table_path)
                logging.info(f"Loaded {table_name} for quality validation")
        
        if not datasets:
            logging.error("No processed datasets found. Run transformation pipeline first.")
            return
        
        # Generate quality report
        quality_report = quality_checker.generate_quality_report(datasets)
        
        # Save reports
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        quality_checker.save_quality_report_csv(quality_report, f"quality_report_{timestamp}")
        quality_checker.save_quality_report_markdown(quality_report, f"quality_report_{timestamp}")
        
        logging.info("Data quality validation completed successfully")
        
    except Exception as e:
        logging.error(f"Data quality pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()