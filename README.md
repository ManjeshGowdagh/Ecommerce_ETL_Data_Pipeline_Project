# E-commerce ETL Data Pipeline(Currently working on)

## Project Overview # ON PROGRESS 

This project implements a scalable ETL (Extract, Transform, Load) data pipeline for processing e-commerce sales data. The pipeline transforms raw transactional data into a clean, analytics-ready star schema that enables business intelligence and reporting.

### Business Use Case
- **Problem Solved**: Convert raw e-commerce transaction data into a structured data warehouse
- **Business Value**: Enable analytics for sales performance, customer insights, and product analysis
- **Target Users**: Data analysts, business intelligence teams, and decision makers

## Tech Stack

- **Python 3.8+**: Core programming language
- **PySpark**: Distributed data processing and transformation
- **Apache Hive**: Data warehouse storage and querying
- **Parquet/ORC**: Columnar storage formats for optimized analytics
- **Power BI**: Business intelligence and visualization
- **SQL**: Data modeling and querying

## Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Data      │    │   Transformation │    │   Star Schema   │
│   (CSV Files)   │───▶│   (PySpark)      │───▶│   (Parquet)     │
│                 │    │                  │    │                 │
│ • orders.csv    │    │ • Data Cleaning  │    │ • Fact_Sales    │
│ • customers.csv │    │ • Validation     │    │ • Dim_Customer  │
│ • products.csv  │    │ • Enrichment     │    │ • Dim_Product   │
│ • payments.csv  │    │ • Standardization│    │ • Dim_Date      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
           │                       │                       │
           ▼                       ▼                       ▼
    Data Ingestion         Data Quality Checks      Business Intelligence
                                   │                       │
                                   ▼                       ▼
                           Quality Reports           Power BI Dashboard
```

## Project Structure

```
├── README.md
├── requirements.txt
├── src/
│   ├── etl/
│   │   ├── ingestion.py      # Data ingestion logic
│   │   ├── transformation.py # PySpark transformation scripts
│   │   └── data_quality.py   # Data quality validation
│   ├── sql/
│   │   ├── create_tables.sql # Star schema table definitions
│   │   └── star_schema.sql   # Data population queries
│   └── utils/
│       └── helpers.py        # Common utility functions
├── data/
│   ├── raw/                  # Source CSV files
│   └── processed/            # Transformed Parquet files
├── config/
│   └── pipeline_config.py    # Configuration settings
├── tests/
│   └── test_data_quality.py  # Unit tests
└── docs/
    ├── pipeline_architecture.md
    └── power_bi_setup.md
```

## Getting Started

### Prerequisites

1. **Python Environment**:
   ```bash
   python -m venv etl_env
   source etl_env/bin/activate  # On Windows: etl_env\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Apache Spark Setup**:
   ```bash
   # Download and set up Spark (version 3.4+)
   export SPARK_HOME=/path/to/spark
   export PATH=$PATH:$SPARK_HOME/bin
   ```

3. **Hive/Hadoop Configuration** (optional for local development):
   - Set up Hadoop and Hive for production deployment
   - Configure HDFS for large-scale data storage

### Running the Pipeline

1. **Data Ingestion**:
   ```bash
   python src/etl/ingestion.py
   ```

2. **Data Transformation**:
   ```bash
   spark-submit src/etl/transformation.py
   ```

3. **Data Quality Validation**:
   ```bash
   python src/etl/data_quality.py
   ```

4. **Star Schema Creation**:
   ```bash
   # Execute SQL scripts in your Hive/database environment
   hive -f src/sql/create_tables.sql
   hive -f src/sql/star_schema.sql
   ```

## Data Quality Metrics

The pipeline includes automated data quality checks:
- **Completeness**: Percentage of null values per column
- **Uniqueness**: Duplicate record detection
- **Validity**: Foreign key constraint validation
- **Accuracy**: Business rule validation (e.g., positive prices)

Quality reports are generated in both CSV and Markdown formats in the `reports/` directory.

## Power BI Integration

Connect Power BI to the transformed data using:
1. **Direct Query**: Connect to Hive/Spark SQL endpoint
2. **Import Mode**: Load Parquet files directly
3. **Gateway**: Use on-premises data gateway for enterprise deployment

See `docs/power_bi_setup.md` for detailed connection instructions.

## Sample Dashboards

The Power BI dashboard includes:
- **Sales Overview**: Total revenue, order count, average order value
- **Product Analysis**: Top-selling products and categories
- **Customer Insights**: Customer segmentation and lifetime value
- **Temporal Trends**: Monthly/quarterly sales patterns

## Performance Considerations

- **Partitioning**: Data is partitioned by date for optimal query performance
- **Compression**: Parquet files use Snappy compression
- **Indexing**: Key columns are indexed for faster lookups
- **Caching**: Frequently accessed datasets are cached in Spark

## Monitoring and Alerting

- Pipeline execution logs are stored in `logs/` directory
- Data quality alerts trigger when thresholds are exceeded
- Performance metrics are tracked for optimization

## Contributing

1. Follow PEP 8 coding standards
2. Add unit tests for new functionality
3. Update documentation for API changes
4. Run data quality checks before merging

## License

This project is licensed under the MIT License - see the LICENSE file for details.
