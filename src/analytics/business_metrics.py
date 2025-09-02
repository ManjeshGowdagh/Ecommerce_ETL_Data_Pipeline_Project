"""
Business metrics calculation module.
Provides key performance indicators and business intelligence metrics.
"""
import logging
from typing import Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    when, desc, asc, round as spark_round, datediff, current_date
)

from config.pipeline_config import CONFIG


class BusinessMetricsCalculator:
    """Calculates key business metrics and KPIs."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
    
    def calculate_revenue_metrics(self, sales_df: DataFrame) -> Dict[str, float]:
        """Calculate revenue-related metrics."""
        metrics = {}
        
        # Total revenue
        total_revenue = sales_df.agg(spark_sum("total_amount")).collect()[0][0] or 0
        metrics["total_revenue"] = round(total_revenue, 2)
        
        # Average order value
        avg_order_value = sales_df.agg(avg("total_amount")).collect()[0][0] or 0
        metrics["average_order_value"] = round(avg_order_value, 2)
        
        # Total orders
        total_orders = sales_df.count()
        metrics["total_orders"] = total_orders
        
        # Revenue per customer
        unique_customers = sales_df.select("customer_id").distinct().count()
        metrics["revenue_per_customer"] = round(total_revenue / unique_customers if unique_customers > 0 else 0, 2)
        
        self.logger.info(f"Revenue metrics calculated: {metrics}")
        return metrics
    
    def calculate_customer_metrics(self, sales_df: DataFrame, customers_df: DataFrame) -> Dict[str, Any]:
        """Calculate customer-related metrics."""
        metrics = {}
        
        # Customer acquisition metrics
        total_customers = customers_df.count()
        metrics["total_customers"] = total_customers
        
        # Customer lifetime value calculation
        customer_ltv = sales_df.groupBy("customer_id") \
            .agg(spark_sum("total_amount").alias("ltv")) \
            .agg(avg("ltv")).collect()[0][0] or 0
        metrics["average_customer_ltv"] = round(customer_ltv, 2)
        
        # Repeat customer rate
        customer_order_counts = sales_df.groupBy("customer_id") \
            .agg(count("order_id").alias("order_count"))
        
        repeat_customers = customer_order_counts.filter(col("order_count") > 1).count()
        metrics["repeat_customer_rate"] = round((repeat_customers / total_customers) * 100 if total_customers > 0 else 0, 2)
        
        # Customer segmentation by value
        customer_segments = sales_df.groupBy("customer_id") \
            .agg(spark_sum("total_amount").alias("total_spent")) \
            .withColumn("segment",
                when(col("total_spent") >= 1000, "High Value")
                .when(col("total_spent") >= 500, "Medium Value")
                .otherwise("Low Value")
            ) \
            .groupBy("segment").count().collect()
        
        metrics["customer_segments"] = {row["segment"]: row["count"] for row in customer_segments}
        
        self.logger.info(f"Customer metrics calculated: {metrics}")
        return metrics
    
    def calculate_product_metrics(self, sales_df: DataFrame, products_df: DataFrame) -> Dict[str, Any]:
        """Calculate product performance metrics."""
        metrics = {}
        
        # Product performance analysis
        product_performance = sales_df.groupBy("product_id") \
            .agg(
                spark_sum("quantity").alias("total_quantity"),
                spark_sum("total_amount").alias("total_revenue"),
                count("order_id").alias("order_count")
            ) \
            .join(products_df.select("product_id", "product_name", "category", "price"), "product_id")
        
        # Top selling products by quantity
        top_products_qty = product_performance.orderBy(desc("total_quantity")).limit(5).collect()
        metrics["top_products_by_quantity"] = [
            {"product_name": row["product_name"], "quantity": row["total_quantity"]}
            for row in top_products_qty
        ]
        
        # Top selling products by revenue
        top_products_revenue = product_performance.orderBy(desc("total_revenue")).limit(5).collect()
        metrics["top_products_by_revenue"] = [
            {"product_name": row["product_name"], "revenue": round(row["total_revenue"], 2)}
            for row in top_products_revenue
        ]
        
        # Category performance
        category_performance = sales_df.join(products_df.select("product_id", "category"), "product_id") \
            .groupBy("category") \
            .agg(spark_sum("total_amount").alias("category_revenue")) \
            .orderBy(desc("category_revenue")).collect()
        
        metrics["category_performance"] = [
            {"category": row["category"], "revenue": round(row["category_revenue"], 2)}
            for row in category_performance
        ]
        
        self.logger.info(f"Product metrics calculated: {metrics}")
        return metrics
    
    def calculate_operational_metrics(self, sales_df: DataFrame, payments_df: DataFrame) -> Dict[str, Any]:
        """Calculate operational and payment metrics."""
        metrics = {}
        
        # Payment method distribution
        payment_distribution = sales_df.groupBy("payment_type") \
            .agg(count("order_id").alias("count")) \
            .collect()
        
        total_payments = sum(row["count"] for row in payment_distribution)
        metrics["payment_method_distribution"] = {
            row["payment_type"]: {
                "count": row["count"],
                "percentage": round((row["count"] / total_payments) * 100, 2)
            }
            for row in payment_distribution
        }
        
        # Order fulfillment metrics
        order_status_dist = sales_df.groupBy("order_status") \
            .agg(count("order_id").alias("count")) \
            .collect()
        
        metrics["order_status_distribution"] = {
            row["order_status"]: row["count"] for row in order_status_dist
        }
        
        # Average processing time (simulated)
        metrics["avg_processing_time_hours"] = 24  # Placeholder for actual calculation
        
        self.logger.info(f"Operational metrics calculated: {metrics}")
        return metrics
    
    def generate_comprehensive_report(self, datasets: Dict[str, DataFrame]) -> Dict[str, Any]:
        """Generate comprehensive business metrics report."""
        report = {
            "timestamp": str(current_date()),
            "pipeline_version": "1.0.0",
            "data_sources": list(datasets.keys())
        }
        
        # Calculate all metrics
        if "orders_clean" in datasets and "customers_clean" in datasets:
            report["revenue_metrics"] = self.calculate_revenue_metrics(datasets["orders_clean"])
            report["customer_metrics"] = self.calculate_customer_metrics(
                datasets["orders_clean"], datasets["customers_clean"]
            )
        
        if "orders_clean" in datasets and "products_clean" in datasets:
            report["product_metrics"] = self.calculate_product_metrics(
                datasets["orders_clean"], datasets["products_clean"]
            )
        
        if "orders_clean" in datasets and "payments_clean" in datasets:
            report["operational_metrics"] = self.calculate_operational_metrics(
                datasets["orders_clean"], datasets["payments_clean"]
            )
        
        return report
    
    def save_metrics_report(self, report: Dict[str, Any], filename: str) -> None:
        """Save business metrics report to file."""
        import json
        import os
        
        os.makedirs(CONFIG.reports_path, exist_ok=True)
        report_path = f"{CONFIG.reports_path}/{filename}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Business metrics report saved to {report_path}")


def main():
    """Main business metrics calculation execution."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{CONFIG.logs_path}/business_metrics.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"{CONFIG.spark_app_name}_BusinessMetrics") \
        .master(CONFIG.spark_master) \
        .config("spark.sql.warehouse.dir", CONFIG.spark_sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Initialize metrics calculator
        metrics_calculator = BusinessMetricsCalculator(spark)
        
        # Load processed datasets
        datasets = {}
        processed_path = CONFIG.processed_data_path
        
        for table_name in ["orders_clean", "customers_clean", "products_clean", "payments_clean"]:
            table_path = f"{processed_path}/{table_name}"
            if os.path.exists(table_path):
                datasets[table_name] = spark.read.parquet(table_path)
                logging.info(f"Loaded {table_name} for metrics calculation")
        
        if not datasets:
            logging.error("No processed datasets found. Run transformation pipeline first.")
            return
        
        # Generate comprehensive metrics report
        metrics_report = metrics_calculator.generate_comprehensive_report(datasets)
        
        # Save report
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metrics_calculator.save_metrics_report(metrics_report, f"business_metrics_{timestamp}")
        
        logging.info("Business metrics calculation completed successfully")
        
    except Exception as e:
        logging.error(f"Business metrics pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()