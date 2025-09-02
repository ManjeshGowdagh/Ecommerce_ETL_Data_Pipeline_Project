"""
Dashboard data preparation module.
Generates aggregated datasets optimized for Power BI consumption.
"""
import os
import logging
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    when, desc, asc, round as spark_round, date_format, year, month
)

from config.pipeline_config import CONFIG


class DashboardDataGenerator:
    """Generates pre-aggregated data for dashboard consumption."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
    
    def create_sales_summary(self, sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """Create sales summary by product category."""
        sales_summary = sales_df.join(products_df.select("product_id", "category", "product_name"), "product_id") \
            .groupBy("category") \
            .agg(
                spark_sum("total_amount").alias("total_revenue"),
                spark_sum("quantity").alias("total_quantity"),
                count("order_id").alias("total_orders"),
                avg("total_amount").alias("avg_order_value")
            ) \
            .withColumn("total_revenue", spark_round("total_revenue", 2)) \
            .withColumn("avg_order_value", spark_round("avg_order_value", 2)) \
            .orderBy(desc("total_revenue"))
        
        return sales_summary
    
    def create_monthly_trends(self, sales_df: DataFrame) -> DataFrame:
        """Create monthly revenue trends."""
        monthly_trends = sales_df \
            .withColumn("year_month", date_format(col("order_date"), "yyyy-MM")) \
            .withColumn("year", year(col("order_date"))) \
            .withColumn("month", month(col("order_date"))) \
            .groupBy("year_month", "year", "month") \
            .agg(
                spark_sum("total_amount").alias("monthly_revenue"),
                count("order_id").alias("monthly_orders"),
                avg("total_amount").alias("avg_order_value")
            ) \
            .withColumn("monthly_revenue", spark_round("monthly_revenue", 2)) \
            .withColumn("avg_order_value", spark_round("avg_order_value", 2)) \
            .orderBy("year", "month")
        
        return monthly_trends
    
    def create_customer_analysis(self, sales_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        """Create customer spending analysis."""
        customer_analysis = sales_df.groupBy("customer_id") \
            .agg(
                spark_sum("total_amount").alias("total_spent"),
                count("order_id").alias("total_orders"),
                avg("total_amount").alias("avg_order_value"),
                spark_max("order_date").alias("last_order_date")
            ) \
            .join(customers_df.select("customer_id", "customer_name", "age_group", "state"), "customer_id") \
            .withColumn("total_spent", spark_round("total_spent", 2)) \
            .withColumn("avg_order_value", spark_round("avg_order_value", 2)) \
            .withColumn("customer_segment",
                when(col("total_spent") >= 1000, "High Value")
                .when(col("total_spent") >= 500, "Medium Value")
                .otherwise("Low Value")
            ) \
            .orderBy(desc("total_spent"))
        
        return customer_analysis
    
    def create_payment_analysis(self, sales_df: DataFrame) -> DataFrame:
        """Create payment method analysis."""
        payment_analysis = sales_df.groupBy("payment_type") \
            .agg(
                count("order_id").alias("transaction_count"),
                spark_sum("total_amount").alias("total_amount"),
                avg("total_amount").alias("avg_transaction_value")
            ) \
            .withColumn("total_amount", spark_round("total_amount", 2)) \
            .withColumn("avg_transaction_value", spark_round("avg_transaction_value", 2)) \
            .orderBy(desc("total_amount"))
        
        # Calculate percentages
        total_transactions = payment_analysis.agg(spark_sum("transaction_count")).collect()[0][0]
        
        payment_analysis = payment_analysis.withColumn(
            "percentage",
            spark_round((col("transaction_count") / total_transactions) * 100, 2)
        )
        
        return payment_analysis
    
    def create_product_performance(self, sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """Create product performance analysis."""
        product_performance = sales_df.groupBy("product_id") \
            .agg(
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("unit_price").alias("avg_selling_price")
            ) \
            .join(products_df.select("product_id", "product_name", "category", "brand", "cost"), "product_id") \
            .withColumn("total_revenue", spark_round("total_revenue", 2)) \
            .withColumn("avg_selling_price", spark_round("avg_selling_price", 2)) \
            .withColumn("profit_per_unit", spark_round(col("avg_selling_price") - col("cost"), 2)) \
            .withColumn("total_profit", spark_round(col("profit_per_unit") * col("total_quantity_sold"), 2)) \
            .orderBy(desc("total_revenue"))
        
        return product_performance
    
    def generate_dashboard_datasets(self, datasets: Dict[str, DataFrame]) -> None:
        """Generate all dashboard datasets."""
        dashboard_path = f"{CONFIG.processed_data_path}/dashboard"
        os.makedirs(dashboard_path, exist_ok=True)
        
        if "orders_clean" not in datasets:
            self.logger.error("Orders dataset not found")
            return
        
        orders_df = datasets["orders_clean"]
        
        # Sales summary by category
        if "products_clean" in datasets:
            sales_summary = self.create_sales_summary(orders_df, datasets["products_clean"])
            self.save_dashboard_data(sales_summary, "sales_by_category", dashboard_path)
            
            # Product performance
            product_performance = self.create_product_performance(orders_df, datasets["products_clean"])
            self.save_dashboard_data(product_performance, "product_performance", dashboard_path)
        
        # Monthly trends
        monthly_trends = self.create_monthly_trends(orders_df)
        self.save_dashboard_data(monthly_trends, "monthly_trends", dashboard_path)
        
        # Customer analysis
        if "customers_clean" in datasets:
            customer_analysis = self.create_customer_analysis(orders_df, datasets["customers_clean"])
            self.save_dashboard_data(customer_analysis, "customer_analysis", dashboard_path)
        
        # Payment analysis
        payment_analysis = self.create_payment_analysis(orders_df)
        self.save_dashboard_data(payment_analysis, "payment_analysis", dashboard_path)
        
        self.logger.info("Dashboard datasets generated successfully")
    
    def save_dashboard_data(self, df: DataFrame, dataset_name: str, output_path: str) -> None:
        """Save dashboard dataset to Parquet format."""
        try:
            file_path = f"{output_path}/{dataset_name}"
            
            df.write \
                .mode("overwrite") \
                .option("compression", CONFIG.compression) \
                .parquet(file_path)
            
            # Also save as CSV for easy Power BI import
            csv_path = f"{output_path}/{dataset_name}.csv"
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(csv_path)
            
            self.logger.info(f"Dashboard dataset saved: {dataset_name}")
            
        except Exception as e:
            self.logger.error(f"Error saving dashboard dataset {dataset_name}: {str(e)}")
    
    def create_kpi_summary(self, datasets: Dict[str, DataFrame]) -> Dict[str, Any]:
        """Create KPI summary for executive dashboard."""
        kpis = {}
        
        if "orders_clean" in datasets:
            orders_df = datasets["orders_clean"]
            
            # Revenue KPIs
            total_revenue = orders_df.agg(spark_sum("total_amount")).collect()[0][0] or 0
            total_orders = orders_df.count()
            avg_order_value = orders_df.agg(avg("total_amount")).collect()[0][0] or 0
            
            kpis["revenue"] = {
                "total_revenue": round(total_revenue, 2),
                "total_orders": total_orders,
                "average_order_value": round(avg_order_value, 2)
            }
            
            # Customer KPIs
            unique_customers = orders_df.select("customer_id").distinct().count()
            kpis["customers"] = {
                "total_customers": unique_customers,
                "revenue_per_customer": round(total_revenue / unique_customers if unique_customers > 0 else 0, 2)
            }
            
            # Product KPIs
            if "products_clean" in datasets:
                products_sold = orders_df.join(datasets["products_clean"], "product_id") \
                    .select("category").distinct().count()
                kpis["products"] = {
                    "categories_sold": products_sold,
                    "total_quantity_sold": orders_df.agg(spark_sum("quantity")).collect()[0][0] or 0
                }
        
        return kpis


def main():
    """Main dashboard data generation execution."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{CONFIG.logs_path}/dashboard_generator.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"{CONFIG.spark_app_name}_DashboardGenerator") \
        .master(CONFIG.spark_master) \
        .config("spark.sql.warehouse.dir", CONFIG.spark_sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Initialize dashboard generator
        dashboard_generator = DashboardDataGenerator(spark)
        
        # Load processed datasets
        datasets = {}
        processed_path = CONFIG.processed_data_path
        
        for table_name in ["orders_clean", "customers_clean", "products_clean", "payments_clean"]:
            table_path = f"{processed_path}/{table_name}"
            if os.path.exists(table_path):
                datasets[table_name] = spark.read.parquet(table_path)
                logging.info(f"Loaded {table_name} for dashboard generation")
        
        if not datasets:
            logging.error("No processed datasets found. Run transformation pipeline first.")
            return
        
        # Generate dashboard datasets
        dashboard_generator.generate_dashboard_datasets(datasets)
        
        # Generate KPI summary
        kpi_summary = dashboard_generator.create_kpi_summary(datasets)
        
        # Save KPI summary
        import json
        kpi_path = f"{CONFIG.reports_path}/kpi_summary.json"
        os.makedirs(CONFIG.reports_path, exist_ok=True)
        
        with open(kpi_path, 'w') as f:
            json.dump(kpi_summary, f, indent=2)
        
        logging.info("Dashboard data generation completed successfully")
        
    except Exception as e:
        logging.error(f"Dashboard generation pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()