# ETL Pipeline Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the e-commerce ETL pipeline in different environments, from local development to production clusters.

## Environment Types

### 1. Local Development Environment

**Purpose**: Development, testing, and small-scale data processing

**Requirements**:
- Python 3.8+
- Apache Spark (local mode)
- 8GB+ RAM recommended
- 10GB+ free disk space

**Setup Steps**:

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   ```bash
   cp .env.example .env
   # Edit .env with local settings
   ```

3. **Run Setup Script**:
   ```bash
   python scripts/setup_environment.py
   ```

4. **Execute Pipeline**:
   ```bash
   python run_pipeline.py
   ```

### 2. Staging Environment

**Purpose**: Integration testing and validation before production

**Requirements**:
- Hadoop cluster (3+ nodes)
- Apache Spark cluster
- Hive metastore
- Shared storage (HDFS/S3)

**Configuration Changes**:
```python
# config/pipeline_config.py
ENVIRONMENT = "staging"
SPARK_MASTER = "yarn"
RAW_DATA_PATH = "hdfs://namenode:9000/data/raw"
PROCESSED_DATA_PATH = "hdfs://namenode:9000/data/processed"
```

### 3. Production Environment

**Purpose**: Production data processing and analytics

**Requirements**:
- High-availability Hadoop cluster
- Apache Spark cluster with resource management
- Enterprise-grade storage
- Monitoring and alerting systems

## Deployment Architecture

### Cloud Deployment (AWS)

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Cloud Environment                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │     S3      │    │    EMR      │    │   Redshift  │         │
│  │ (Raw Data)  │───▶│ (Processing)│───▶│(Data Warehouse)       │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
│         │                   │                   │               │
│         ▼                   ▼                   ▼               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │ Lambda      │    │ CloudWatch  │    │ QuickSight  │         │
│  │(Triggers)   │    │(Monitoring) │    │(Analytics)  │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

**AWS Services Configuration**:

1. **Amazon S3**:
   ```bash
   # Create S3 buckets
   aws s3 mb s3://ecommerce-etl-raw-data
   aws s3 mb s3://ecommerce-etl-processed-data
   aws s3 mb s3://ecommerce-etl-reports
   ```

2. **Amazon EMR**:
   ```json
   {
     "Name": "EcommerceETLCluster",
     "ReleaseLabel": "emr-6.10.0",
     "Applications": [
       {"Name": "Spark"},
       {"Name": "Hive"},
       {"Name": "Hadoop"}
     ],
     "Instances": {
       "MasterInstanceType": "m5.xlarge",
       "SlaveInstanceType": "m5.large",
       "InstanceCount": 3
     }
   }
   ```

3. **AWS Lambda Triggers**:
   ```python
   import boto3
   
   def lambda_handler(event, context):
       emr = boto3.client('emr')
       
       # Trigger ETL pipeline
       response = emr.add_job_flow_steps(
           JobFlowId='j-XXXXXXXXXX',
           Steps=[{
               'Name': 'ETL Pipeline Execution',
               'ActionOnFailure': 'TERMINATE_CLUSTER',
               'HadoopJarStep': {
                   'Jar': 'command-runner.jar',
                   'Args': ['spark-submit', 's3://scripts/run_pipeline.py']
               }
           }]
       )
       
       return response
   ```

### On-Premises Deployment

**Infrastructure Requirements**:
- Hadoop cluster (minimum 3 nodes)
- Apache Spark cluster
- Hive metastore database
- Network-attached storage

**Deployment Steps**:

1. **Cluster Setup**:
   ```bash
   # Install Hadoop
   wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
   tar -xzf hadoop-3.3.4.tar.gz
   
   # Install Spark
   wget https://downloads.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
   tar -xzf spark-3.4.0-bin-hadoop3.tgz
   ```

2. **Configuration Files**:
   ```xml
   <!-- core-site.xml -->
   <configuration>
     <property>
       <name>fs.defaultFS</name>
       <value>hdfs://namenode:9000</value>
     </property>
   </configuration>
   ```

3. **Deploy Pipeline Code**:
   ```bash
   # Copy pipeline code to cluster
   scp -r src/ user@namenode:/opt/etl-pipeline/
   
   # Set up Python environment on all nodes
   ansible-playbook -i inventory deploy-python-env.yml
   ```

## Container Deployment

### Docker Configuration

**Dockerfile**:
```dockerfile
FROM openjdk:11-jre-slim

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Install Spark
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy application code
COPY . /app
WORKDIR /app

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Set entrypoint
ENTRYPOINT ["python3", "run_pipeline.py"]
```

**Docker Compose**:
```yaml
version: '3.8'

services:
  etl-pipeline:
    build: .
    volumes:
      - ./data:/app/data
      - ./reports:/app/reports
      - ./logs:/app/logs
    environment:
      - ENVIRONMENT=production
      - SPARK_MASTER=local[*]
    depends_on:
      - hive-metastore
  
  hive-metastore:
    image: apache/hive:3.1.3
    environment:
      - SERVICE_NAME=metastore
    ports:
      - "9083:9083"
```

### Kubernetes Deployment

**Deployment Manifest**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etl-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: etl-pipeline
  template:
    metadata:
      labels:
        app: etl-pipeline
    spec:
      containers:
      - name: etl-pipeline
        image: ecommerce-etl:latest
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        env:
        - name: ENVIRONMENT
          value: "production"
        - name: SPARK_MASTER
          value: "k8s://https://kubernetes.default.svc:443"
        volumeMounts:
        - name: data-volume
          mountPath: /app/data
        - name: reports-volume
          mountPath: /app/reports
      volumes:
      - name: data-volume
        persistentVolumeClaim:
          claimName: etl-data-pvc
      - name: reports-volume
        persistentVolumeClaim:
          claimName: etl-reports-pvc
```

## Monitoring and Alerting

### Prometheus Metrics

```python
# Add to pipeline code
from prometheus_client import Counter, Histogram, Gauge

# Define metrics
pipeline_executions = Counter('pipeline_executions_total', 'Total pipeline executions')
pipeline_duration = Histogram('pipeline_duration_seconds', 'Pipeline execution duration')
data_quality_score = Gauge('data_quality_score', 'Current data quality score')
```

### Grafana Dashboard

**Key Metrics to Monitor**:
- Pipeline execution frequency and duration
- Data quality scores over time
- Error rates and failure patterns
- Resource utilization (CPU, memory, disk)
- Data volume trends

### Alerting Rules

```yaml
# alerting-rules.yml
groups:
- name: etl-pipeline
  rules:
  - alert: PipelineFailure
    expr: increase(pipeline_failures_total[1h]) > 0
    for: 0m
    labels:
      severity: critical
    annotations:
      summary: "ETL Pipeline has failed"
      
  - alert: DataQualityDegraded
    expr: data_quality_score < 90
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Data quality score below threshold"
```

## Security Configuration

### Authentication and Authorization

1. **Kerberos Authentication**:
   ```bash
   # Configure Kerberos for Hadoop security
   kinit etl-service@REALM.COM
   ```

2. **RBAC Configuration**:
   ```yaml
   # Kubernetes RBAC
   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: etl-pipeline-role
   rules:
   - apiGroups: [""]
     resources: ["pods", "services"]
     verbs: ["get", "list", "create", "delete"]
   ```

### Data Encryption

1. **At Rest**:
   - HDFS encryption zones
   - S3 server-side encryption
   - Database encryption

2. **In Transit**:
   - TLS/SSL for all communications
   - Spark encryption configuration
   - Secure Hive connections

## Performance Tuning

### Spark Optimization

```python
# Spark configuration for production
spark_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}
```

### Resource Allocation

```bash
# Submit Spark job with optimized resources
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors  10 \
  --conf spark.sql.adaptive.enabled=true \
  run_pipeline.py
```

## Backup and Recovery

### Data Backup Strategy

1. **Incremental Backups**:
   ```bash
   # Daily incremental backup
   hadoop distcp hdfs://source/data hdfs://backup/data/$(date +%Y%m%d)
   ```

2. **Configuration Backup**:
   ```bash
   # Backup configuration files
   tar -czf config-backup-$(date +%Y%m%d).tar.gz config/ src/
   ```

### Disaster Recovery

1. **Recovery Procedures**:
   - Data restoration from backups
   - Configuration restoration
   - Service restart procedures

2. **Testing**:
   - Regular disaster recovery drills
   - Automated recovery testing
   - Documentation updates

## Maintenance Procedures

### Regular Maintenance Tasks

1. **Daily**:
   - Monitor pipeline execution
   - Check data quality reports
   - Review error logs

2. **Weekly**:
   - Performance analysis
   - Capacity planning review
   - Security audit

3. **Monthly**:
   - Full system backup
   - Configuration review
   - Documentation updates

### Troubleshooting Guide

**Common Issues**:

1. **Out of Memory Errors**:
   ```bash
   # Increase driver memory
   --driver-memory 8g
   # Increase executor memory
   --executor-memory 16g
   ```

2. **Slow Performance**:
   ```python
   # Enable adaptive query execution
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   # Optimize partition size
   spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
   ```

3. **Connection Issues**:
   ```bash
   # Test Hive connectivity
   beeline -u "jdbc:hive2://hiveserver:10000" -e "SHOW DATABASES;"
   ```

## Scaling Strategies

### Horizontal Scaling

1. **Add Cluster Nodes**:
   ```bash
   # Add worker nodes to existing cluster
   yarn rmadmin -addToClusterNodeLabels "worker-node-3"
   ```

2. **Auto-scaling Configuration**:
   ```yaml
   # Kubernetes HPA
   apiVersion: autoscaling/v2
   kind: HorizontalPodAutoscaler
   metadata:
     name: etl-pipeline-hpa
   spec:
     scaleTargetRef:
       apiVersion: apps/v1
       kind: Deployment
       name: etl-pipeline
     minReplicas: 1
     maxReplicas: 10
     metrics:
     - type: Resource
       resource:
         name: cpu
         target:
           type: Utilization
           averageUtilization: 70
   ```

### Vertical Scaling

1. **Resource Optimization**:
   - Memory allocation tuning
   - CPU core optimization
   - Storage performance tuning

2. **Configuration Tuning**:
   ```python
   # Optimize for large datasets
   spark.conf.set("spark.sql.shuffle.partitions", "400")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
   ```

## Cost Optimization

### Resource Management

1. **Spot Instances** (AWS):
   ```json
   {
     "InstanceFleets": [{
       "InstanceFleetType": "CORE",
       "TargetSpotCapacity": 2,
       "LaunchSpecifications": {
         "SpotSpecification": {
           "TimeoutDurationMinutes": 60,
           "TimeoutAction": "TERMINATE_CLUSTER"
         }
       }
     }]
   }
   ```

2. **Auto-termination**:
   ```python
   # Automatic cluster termination after idle time
   cluster_config = {
       "AutoTerminationPolicy": {
           "IdleTimeout": 3600  # 1 hour
       }
   }
   ```

### Storage Optimization

1. **Data Lifecycle Management**:
   ```python
   # Archive old data to cheaper storage
   def archive_old_data(cutoff_date):
       old_data_path = f"hdfs://data/archive/{cutoff_date}"
       # Move data older than cutoff_date
   ```

2. **Compression Strategies**:
   ```python
   # Use efficient compression
   df.write.option("compression", "gzip").parquet(output_path)
   ```

## Compliance and Governance

### Data Governance

1. **Data Lineage Tracking**:
   ```python
   # Track data transformations
   lineage_metadata = {
       "source_tables": ["orders", "customers"],
       "target_table": "fact_sales",
       "transformation_logic": "join_and_aggregate",
       "execution_timestamp": datetime.now()
   }
   ```

2. **Data Catalog Integration**:
   - Apache Atlas integration
   - Metadata management
   - Schema evolution tracking

### Compliance Requirements

1. **GDPR Compliance**:
   - Data anonymization procedures
   - Right to be forgotten implementation
   - Consent management

2. **SOX Compliance**:
   - Audit trail maintenance
   - Change control procedures
   - Access logging

## Support and Maintenance

### Support Procedures

1. **Incident Response**:
   - 24/7 monitoring setup
   - Escalation procedures
   - Communication protocols

2. **Change Management**:
   - Code review processes
   - Deployment approval workflows
   - Rollback procedures

### Documentation Maintenance

1. **Keep Updated**:
   - Architecture diagrams
   - Configuration documentation
   - Troubleshooting guides

2. **Version Control**:
   - Document all changes
   - Maintain deployment history
   - Track configuration changes